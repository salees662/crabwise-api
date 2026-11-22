/**
 * Media upload service.
 *
 * Issues a presigned S3 upload URL and creates the corresponding `Media`
 * record. The client uploads the file bytes directly to S3 using the
 * returned URL; this service never touches the file body.
 */

import { env } from '@/config';
import { AppError } from '@/middlewares';
import { enqueueImageProcessing } from '@/queues';
import { prisma } from '@/services';
import {
  ALLOWED_DIGITAL_FILE_MIME_TYPES,
  ALLOWED_IMAGE_MIME_TYPES,
  MAX_DIGITAL_FILE_SIZE_BYTES,
  MAX_IMAGE_SIZE_BYTES,
} from '@/validators/media.schemas';

import {
  buildObjectKey,
  buildPublicUrl,
  createPresignedUploadUrl,
  deleteObject,
  objectExists,
} from './s3.service';

export interface PresignUploadInput {
  readonly type: 'IMAGE' | 'DIGITAL_FILE';
  readonly mimeType: string;
  readonly filename: string;
  readonly size: number;
  readonly parentType?: 'ARTWORK' | 'PORTFOLIO';
  readonly parentId?: string;
}

export interface PresignUploadResult {
  readonly mediaId: string;
  readonly uploadUrl: string;
  readonly key: string;
  readonly expiresInSeconds: number;
}

const PRESIGNED_UPLOAD_TTL_SECONDS = 300;

function throwMediaNotFound(): never {
  throw new AppError('NOT_FOUND', 'Media not found');
}

async function assertParentOwnership(
  userId: string,
  parentType: 'ARTWORK' | 'PORTFOLIO',
  parentId: string,
): Promise<void> {
  if (parentType === 'ARTWORK') {
    const artwork = await prisma.artwork.findUnique({ where: { id: parentId } });
    if (artwork === null) {
      throw new AppError('NOT_FOUND', 'Artwork not found');
    }
    if (artwork.userId !== userId) {
      throw new AppError('FORBIDDEN', 'You do not own this artwork');
    }
  } else {
    const item = await prisma.portfolioItem.findUnique({ where: { id: parentId } });
    if (item === null) {
      throw new AppError('NOT_FOUND', 'Portfolio item not found');
    }
    if (item.userId !== userId) {
      throw new AppError('FORBIDDEN', 'You do not own this portfolio item');
    }
  }
}

/**
 * Creates the `Media` record and, when a parent is given, links it via the
 * appropriate join table — before the file itself has been uploaded. The
 * presigned URL expires in `PRESIGNED_UPLOAD_TTL_SECONDS`; if the client
 * never uploads, the `Media` row is orphaned and handled by the same
 * cleanup sweep described in the schema for deleted parents.
 */
export async function presignMediaUpload(
  userId: string,
  input: PresignUploadInput,
): Promise<PresignUploadResult> {
  const allowedMimeTypes: readonly string[] =
    input.type === 'IMAGE' ? ALLOWED_IMAGE_MIME_TYPES : ALLOWED_DIGITAL_FILE_MIME_TYPES;
  if (!allowedMimeTypes.includes(input.mimeType)) {
    throw new AppError('BAD_REQUEST', 'Unsupported media MIME type');
  }

  const maxSize = input.type === 'IMAGE' ? MAX_IMAGE_SIZE_BYTES : MAX_DIGITAL_FILE_SIZE_BYTES;
  if (!Number.isSafeInteger(input.size) || input.size <= 0 || input.size > maxSize) {
    throw new AppError(
      'BAD_REQUEST',
      `Media size must be a positive integer no greater than ${maxSize} bytes`,
    );
  }

  if (input.parentType !== undefined && input.parentId !== undefined) {
    await assertParentOwnership(userId, input.parentType, input.parentId);
  }

  const folder = input.type === 'IMAGE' ? 'images' : 'digital-files';
  const key = buildObjectKey(userId, folder, input.filename);
  const uploadUrl = await createPresignedUploadUrl(key, input.mimeType);

  const media = await prisma.media.create({
    data: {
      userId,
      type: input.type,
      bucket: env.s3Bucket ?? '',
      key,
      mimeType: input.mimeType,
      size: BigInt(input.size),
    },
  });

  if (input.parentType === 'ARTWORK' && input.parentId !== undefined) {
    await prisma.artworkMedia.create({
      data: { artworkId: input.parentId, mediaId: media.id },
    });
  } else if (input.parentType === 'PORTFOLIO' && input.parentId !== undefined) {
    await prisma.portfolioMedia.create({
      data: { portfolioItemId: input.parentId, mediaId: media.id },
    });
  }

  return {
    mediaId: media.id,
    uploadUrl,
    key,
    expiresInSeconds: PRESIGNED_UPLOAD_TTL_SECONDS,
  };
}

export interface MediaVariant {
  readonly label: string;
  readonly url: string;
  readonly width: number | null;
  readonly height: number | null;
}

export interface MediaRecord {
  readonly id: string;
  readonly type: 'IMAGE' | 'DIGITAL_FILE';
  readonly url: string;
  readonly mimeType: string;
  readonly size: string;
  readonly width: number | null;
  readonly height: number | null;
  readonly variants: readonly MediaVariant[];
  readonly createdAt: Date;
}

const GENERATED_IMAGE_VARIANT_LABELS = ['thumbnail', 'webp'] as const;

async function toMediaRecord(media: {
  id: string;
  type: 'IMAGE' | 'DIGITAL_FILE';
  key: string;
  mimeType: string;
  size: bigint;
  width: number | null;
  height: number | null;
  createdAt: Date;
}): Promise<MediaRecord> {
  const variants = await prisma.media.findMany({ where: { variantOfId: media.id } });
  const variantsByLabel = new Map(
    variants.filter((v) => v.variantLabel !== null).map((v) => [v.variantLabel as string, v]),
  );
  const completeVariants = GENERATED_IMAGE_VARIANT_LABELS.every((label) =>
    variantsByLabel.has(label),
  );
  const persistedVariants = completeVariants
    ? GENERATED_IMAGE_VARIANT_LABELS.map((label) => variantsByLabel.get(label)!)
    : [];

  return {
    id: media.id,
    type: media.type,
    url: buildPublicUrl(media.key),
    mimeType: media.mimeType,
    size: media.size.toString(),
    width: media.width,
    height: media.height,
    createdAt: media.createdAt,
    variants: persistedVariants.map((v) => ({
      label: v.variantLabel ?? 'variant',
      url: buildPublicUrl(v.key),
      width: v.width,
      height: v.height,
    })),
  };
}

/**
 * Whether `confirmMediaUpload` should HEAD-check S3 before enqueueing image
 * processing (#873). On by default in production (wasted queue jobs and
 * delayed error feedback hurt most there); off elsewhere unless
 * `VERIFY_S3_UPLOAD=true` is set explicitly.
 */
function shouldVerifyUploadExists(): boolean {
  return env.verifyS3Upload ?? env.isProduction;
}

/**
 * Marks an upload as finished and, for images, enqueues thumbnail/WebP
 * generation. When verification is enabled (#873) the object is HEAD-checked
 * in S3 first, so a client that never completed the presigned upload gets an
 * immediate, actionable error instead of a silently-failing background job.
 * Verification is skipped on unexpected S3 errors (fail open — the worker
 * still handles a genuinely missing object gracefully).
 */
export async function confirmMediaUpload(userId: string, mediaId: string): Promise<MediaRecord> {
  const media = await prisma.media.findUnique({ where: { id: mediaId } });
  if (media === null) {
    throwMediaNotFound();
  }
  if (media.userId !== userId) {
    throw new AppError('FORBIDDEN', 'You do not own this media');
  }

  if (media.type === 'IMAGE' && media.variantOfId === null) {
    if (shouldVerifyUploadExists()) {
      let exists: boolean;
      try {
        exists = await objectExists(media.key);
      } catch {
        // Transient S3 error — don't block confirm on infrastructure flake;
        // the worker still fails gracefully if the object really is missing.
        exists = true;
      }
      if (!exists) {
        await prisma.media.deleteMany({ where: { id: media.id } });
        throw new AppError(
          'UNPROCESSABLE_ENTITY',
          'The upload did not complete — upload the file again before confirming',
        );
      }
    }
    await enqueueImageProcessing(media.id);
  }

  return toMediaRecord(media);
}

export async function getMediaRecord(mediaId: string): Promise<MediaRecord> {
  const media = await prisma.media.findUnique({ where: { id: mediaId } });
  if (media === null) {
    throwMediaNotFound();
  }
  return toMediaRecord(media);
}

export async function deleteMedia(userId: string, mediaId: string): Promise<void> {
  const media = await prisma.media.findUnique({ where: { id: mediaId } });
  if (media === null) {
    throw new AppError('NOT_FOUND', 'Media not found');
  }
  if (media.userId !== userId) {
    throw new AppError('FORBIDDEN', 'You do not own this media');
  }

  const variants = await prisma.media.findMany({ where: { variantOfId: media.id } });
  for (const item of [media, ...variants]) {
    await deleteObject(item.bucket, item.key);
  }
  await prisma.media.deleteMany({
    where: { OR: [{ id: media.id }, { variantOfId: media.id }] },
  });
}
