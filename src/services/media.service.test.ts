import { beforeEach, describe, expect, it, vi } from 'vitest';

const MEDIA_ID = 'media-1';
const USER_ID = 'user-1';

const {
  prismaMock,
  envMock,
  enqueueMock,
  objectExistsMock,
  createPresignedMock,
  s3UploadMock,
  s3DownloadMock,
  s3DeleteMock,
} = vi.hoisted(() => ({
  prismaMock: {
    media: {
      findUnique: vi.fn(),
      findMany: vi.fn(),
      create: vi.fn(),
      deleteMany: vi.fn(),
    },
    artworkMedia: { create: vi.fn() },
    portfolioMedia: { create: vi.fn() },
  },
  envMock: {
    s3Bucket: 'test-bucket',
    s3Region: 'us-east-1',
    isProduction: false,
    verifyS3Upload: undefined as boolean | undefined,
    redisUrl: 'redis://localhost:6379',
  },
  enqueueMock: vi.fn(),
  objectExistsMock: vi.fn(),
  createPresignedMock: vi.fn(),
  s3UploadMock: vi.fn(),
  s3DownloadMock: vi.fn(),
  s3DeleteMock: vi.fn(),
}));

vi.mock('@/config', () => ({ env: envMock }));
vi.mock('@/services', () => ({ prisma: prismaMock }));
vi.mock('@/queues', () => ({ enqueueImageProcessing: enqueueMock }));
vi.mock('./s3.service', () => ({
  buildObjectKey: vi.fn(
    (userId: string, folder: string, name: string) => `${folder}/${userId}/${name}`,
  ),
  buildPublicUrl: vi.fn((key: string) => `https://cdn/${key}`),
  createPresignedUploadUrl: createPresignedMock,
  objectExists: objectExistsMock,
  downloadObjectBuffer: s3DownloadMock,
  uploadObjectBuffer: s3UploadMock,
  deleteObject: s3DeleteMock,
}));
vi.mock('sharp', () => ({
  default: vi.fn(() => ({
    resize: vi.fn().mockReturnThis(),
    webp: vi.fn().mockReturnThis(),
    toBuffer: vi.fn().mockResolvedValue({
      data: Buffer.from('thumb'),
      info: { width: 300, height: 200 },
    }),
  })),
}));

import {
  confirmMediaUpload,
  deleteMedia,
  getMediaRecord,
  presignMediaUpload,
} from './media.service';
import { processImage } from '../workers/media.worker';
import {
  ALLOWED_DIGITAL_FILE_MIME_TYPES,
  ALLOWED_IMAGE_MIME_TYPES,
  MAX_DIGITAL_FILE_SIZE_BYTES,
  MAX_IMAGE_SIZE_BYTES,
  presignUploadSchema,
} from '../validators/media.schemas';
import type { Job } from 'bullmq';
import type { ImageProcessingJobData } from '../queues/media.queue';

function makeMedia(overrides: Record<string, unknown> = {}) {
  return {
    id: MEDIA_ID,
    userId: USER_ID,
    type: 'IMAGE',
    variantOfId: null,
    bucket: 'test-bucket',
    key: 'images/user-1/file.jpg',
    mimeType: 'image/jpeg',
    size: BigInt(1024),
    width: null,
    height: null,
    createdAt: new Date(),
    ...overrides,
  };
}

function makeJob(): Job<ImageProcessingJobData> {
  return {
    id: 'job-1',
    data: { mediaId: MEDIA_ID },
    attemptsMade: 0,
    attemptsStarted: 0,
  } as unknown as Job<ImageProcessingJobData>;
}

beforeEach(() => {
  vi.clearAllMocks();
  envMock.isProduction = false;
  envMock.verifyS3Upload = undefined;
  prismaMock.media.findUnique.mockResolvedValue(makeMedia());
  prismaMock.media.findMany.mockResolvedValue([]);
  prismaMock.media.create.mockResolvedValue({ id: MEDIA_ID });
  enqueueMock.mockResolvedValue(undefined);
  objectExistsMock.mockResolvedValue(true);
  createPresignedMock.mockResolvedValue('https://s3/upload');
  s3DownloadMock.mockResolvedValue(Buffer.from('original-image'));
  s3UploadMock.mockResolvedValue(undefined);
});

describe('presignMediaUpload service validation', () => {
  it.each([
    ...ALLOWED_IMAGE_MIME_TYPES.map((mimeType) => ['IMAGE', mimeType] as const),
    ...ALLOWED_DIGITAL_FILE_MIME_TYPES.map((mimeType) => ['DIGITAL_FILE', mimeType] as const),
  ])('accepts the supported %s MIME type %s', async (type, mimeType) => {
    const result = await presignMediaUpload(USER_ID, {
      type,
      mimeType,
      filename: 'file',
      size: 1024,
    });

    expect(result.mediaId).toBe(MEDIA_ID);
    expect(createPresignedMock).toHaveBeenCalledWith(expect.any(String), mimeType);
  });

  it.each([
    ['IMAGE', 'image/jpeg; charset=utf-8'],
    ['IMAGE', 'image/jpe'],
    ['IMAGE', 'text/image/jpeg'],
    ['IMAGE', ' image/jpeg'],
    ['IMAGE', 'application/pdf'],
    ['DIGITAL_FILE', 'application/pdf; charset=utf-8'],
    ['DIGITAL_FILE', 'image/jpeg'],
  ] as const)(
    'rejects unsupported or malformed %s MIME type %s before writing metadata',
    async (type, mimeType) => {
      await expect(
        presignMediaUpload(USER_ID, {
          type,
          mimeType,
          filename: 'file',
          size: 1024,
        }),
      ).rejects.toMatchObject({ code: 'BAD_REQUEST' });

      expect(prismaMock.media.create).not.toHaveBeenCalled();
      expect(createPresignedMock).not.toHaveBeenCalled();
    },
  );

  it.each([
    ['IMAGE', 'image/jpeg', MAX_IMAGE_SIZE_BYTES],
    ['DIGITAL_FILE', 'application/pdf', MAX_DIGITAL_FILE_SIZE_BYTES],
  ] as const)('accepts a %s upload at its exact size limit', async (type, mimeType, size) => {
    await expect(
      presignMediaUpload(USER_ID, { type, mimeType, filename: 'file', size }),
    ).resolves.toMatchObject({ mediaId: MEDIA_ID });
  });

  it.each([
    ['IMAGE', 'image/jpeg', MAX_IMAGE_SIZE_BYTES + 1],
    ['DIGITAL_FILE', 'application/pdf', MAX_DIGITAL_FILE_SIZE_BYTES + 1],
  ] as const)(
    'rejects a %s upload above its size limit before writing metadata',
    async (type, mimeType, size) => {
      await expect(
        presignMediaUpload(USER_ID, { type, mimeType, filename: 'file', size }),
      ).rejects.toMatchObject({ code: 'BAD_REQUEST' });

      expect(prismaMock.media.create).not.toHaveBeenCalled();
    },
  );

  it('does not create media metadata when presigning fails', async () => {
    createPresignedMock.mockRejectedValue(new Error('S3 signing failed'));

    await expect(
      presignMediaUpload(USER_ID, {
        type: 'IMAGE',
        mimeType: 'image/jpeg',
        filename: 'file',
        size: 1024,
      }),
    ).rejects.toThrow('S3 signing failed');

    expect(prismaMock.media.create).not.toHaveBeenCalled();
  });
});

describe('presignUploadSchema limits', () => {
  const base = {
    type: 'IMAGE' as const,
    mimeType: 'image/jpeg',
    filename: 'a.jpg',
    size: 1024,
  };

  it('accepts a file exactly at the image size limit', () => {
    const result = presignUploadSchema.safeParse({ ...base, size: MAX_IMAGE_SIZE_BYTES });
    expect(result.success).toBe(true);
  });

  it('rejects an image one byte over the limit', () => {
    const result = presignUploadSchema.safeParse({ ...base, size: MAX_IMAGE_SIZE_BYTES + 1 });
    expect(result.success).toBe(false);
    expect(result.error?.issues.map((i) => i.path.join('.'))).toContain('size');
  });

  it('accepts a digital file exactly at its limit but rejects one byte over', () => {
    const digital = { ...base, type: 'DIGITAL_FILE' as const, mimeType: 'application/pdf' };
    expect(
      presignUploadSchema.safeParse({ ...digital, size: MAX_DIGITAL_FILE_SIZE_BYTES }).success,
    ).toBe(true);
    expect(
      presignUploadSchema.safeParse({ ...digital, size: MAX_DIGITAL_FILE_SIZE_BYTES + 1 }).success,
    ).toBe(false);
  });

  it('rejects zero and negative sizes', () => {
    expect(presignUploadSchema.safeParse({ ...base, size: 0 }).success).toBe(false);
    expect(presignUploadSchema.safeParse({ ...base, size: -1 }).success).toBe(false);
  });

  it('rejects non-integer sizes', () => {
    expect(presignUploadSchema.safeParse({ ...base, size: 1.5 }).success).toBe(false);
  });

  it('rejects an unsupported mime type for the upload type', () => {
    const result = presignUploadSchema.safeParse({ ...base, mimeType: 'application/pdf' });
    expect(result.success).toBe(false);
    expect(result.error?.issues.map((i) => i.path.join('.'))).toContain('mimeType');
  });

  it('rejects mime types longer than 255 characters', () => {
    const result = presignUploadSchema.safeParse({ ...base, mimeType: 'a'.repeat(256) });
    expect(result.success).toBe(false);
  });

  it('rejects filenames longer than 255 characters and empty filenames', () => {
    expect(presignUploadSchema.safeParse({ ...base, filename: 'a'.repeat(256) }).success).toBe(
      false,
    );
    expect(presignUploadSchema.safeParse({ ...base, filename: '   ' }).success).toBe(false);
  });

  it('accepts a filename of exactly 255 characters', () => {
    expect(presignUploadSchema.safeParse({ ...base, filename: 'a'.repeat(255) }).success).toBe(
      true,
    );
  });

  it('accepts omitted optional metadata (parentType and parentId)', () => {
    expect(presignUploadSchema.safeParse(base).success).toBe(true);
  });

  it('accepts parentType and parentId provided together', () => {
    const result = presignUploadSchema.safeParse({
      ...base,
      parentType: 'ARTWORK',
      parentId: '123e4567-e89b-42d3-a456-426614174000',
    });
    expect(result.success).toBe(true);
  });

  it('rejects parentType without parentId and vice versa', () => {
    expect(presignUploadSchema.safeParse({ ...base, parentType: 'ARTWORK' }).success).toBe(false);
    expect(
      presignUploadSchema.safeParse({
        ...base,
        parentId: '123e4567-e89b-42d3-a456-426614174000',
      }).success,
    ).toBe(false);
  });

  it('rejects an unknown parentType and a malformed parentId', () => {
    expect(
      presignUploadSchema.safeParse({
        ...base,
        parentType: 'USER',
        parentId: '123e4567-e89b-42d3-a456-426614174000',
      }).success,
    ).toBe(false);
    expect(
      presignUploadSchema.safeParse({ ...base, parentType: 'ARTWORK', parentId: 'not-a-uuid' })
        .success,
    ).toBe(false);
  });
});

describe('confirmMediaUpload S3 verification (#873)', () => {
  it('verifies the object exists before enqueueing when enabled', async () => {
    envMock.verifyS3Upload = true;

    await confirmMediaUpload(USER_ID, MEDIA_ID);

    expect(objectExistsMock).toHaveBeenCalledWith('images/user-1/file.jpg');
    expect(enqueueMock).toHaveBeenCalledWith(MEDIA_ID);
  });

  it('returns a clear error and skips enqueueing when the object is missing', async () => {
    envMock.verifyS3Upload = true;
    objectExistsMock.mockResolvedValue(false);

    await expect(confirmMediaUpload(USER_ID, MEDIA_ID)).rejects.toMatchObject({
      code: 'UNPROCESSABLE_ENTITY',
    });
    expect(enqueueMock).not.toHaveBeenCalled();
    expect(prismaMock.media.deleteMany).toHaveBeenCalledWith({ where: { id: MEDIA_ID } });
  });

  it('defaults to verifying in production without an explicit override', async () => {
    envMock.isProduction = true;

    await confirmMediaUpload(USER_ID, MEDIA_ID);

    expect(objectExistsMock).toHaveBeenCalled();
    expect(enqueueMock).toHaveBeenCalledWith(MEDIA_ID);
  });

  it('defaults to skipping verification outside production', async () => {
    await confirmMediaUpload(USER_ID, MEDIA_ID);

    expect(objectExistsMock).not.toHaveBeenCalled();
    expect(enqueueMock).toHaveBeenCalledWith(MEDIA_ID);
  });

  it('fails open on unexpected S3 errors rather than blocking confirm', async () => {
    envMock.verifyS3Upload = true;
    objectExistsMock.mockRejectedValue(new Error('S3 500'));

    await confirmMediaUpload(USER_ID, MEDIA_ID);

    expect(enqueueMock).toHaveBeenCalledWith(MEDIA_ID);
  });

  it('never verifies or enqueues for non-image media', async () => {
    envMock.verifyS3Upload = true;
    prismaMock.media.findUnique.mockResolvedValue(makeMedia({ type: 'DIGITAL_FILE' }));

    await confirmMediaUpload(USER_ID, MEDIA_ID);

    expect(objectExistsMock).not.toHaveBeenCalled();
    expect(enqueueMock).not.toHaveBeenCalled();
  });

  it('rejects confirmation for media the user does not own', async () => {
    await expect(confirmMediaUpload('someone-else', MEDIA_ID)).rejects.toMatchObject({
      code: 'FORBIDDEN',
    });
    expect(enqueueMock).not.toHaveBeenCalled();
  });
});

describe('complete media variant persistence', () => {
  it('does not expose a partial variant set as complete', async () => {
    prismaMock.media.findMany.mockResolvedValue([
      {
        id: 'thumbnail-1',
        key: 'images/variants/thumbnail/media-1.jpg',
        variantLabel: 'thumbnail',
        width: 300,
        height: 200,
      },
    ]);

    await expect(getMediaRecord(MEDIA_ID)).resolves.toMatchObject({ variants: [] });
  });

  it('exposes both variants only after both metadata rows exist', async () => {
    prismaMock.media.findMany.mockResolvedValue([
      {
        id: 'thumbnail-1',
        key: 'images/variants/thumbnail/media-1.jpg',
        variantLabel: 'thumbnail',
        width: 300,
        height: 200,
      },
      {
        id: 'webp-1',
        key: 'images/variants/webp/media-1.webp',
        variantLabel: 'webp',
        width: 1200,
        height: 800,
      },
    ]);

    const media = await getMediaRecord(MEDIA_ID);

    expect(media.variants).toHaveLength(2);
    expect(media.variants.map((variant) => variant.label)).toEqual(['thumbnail', 'webp']);
  });

  it('surfaces metadata lookup failures instead of returning an incomplete record', async () => {
    prismaMock.media.findMany.mockRejectedValue(new Error('variant metadata unavailable'));

    await expect(getMediaRecord(MEDIA_ID)).rejects.toThrow('variant metadata unavailable');
  });
});

describe('media worker failure surfacing (#97)', () => {
  it('re-throws so BullMQ marks the job failed when the original download fails', async () => {
    s3DownloadMock.mockRejectedValue(new Error('S3 download failed'));

    await expect(processImage(makeJob())).rejects.toThrow('S3 download failed');
  });

  it('re-throws when sharp conversion fails', async () => {
    const sharpMock = (await import('sharp')).default as unknown as ReturnType<typeof vi.fn>;
    sharpMock.mockImplementation(() => {
      throw new Error('sharp: unsupported image format');
    });

    await expect(processImage(makeJob())).rejects.toThrow('sharp: unsupported image format');
  });

  it('re-throws when variant persistence (prisma.media.create) fails', async () => {
    prismaMock.media.create.mockRejectedValue(new Error('prisma write failed'));

    await expect(processImage(makeJob())).rejects.toThrow('prisma write failed');
  });

  it('re-throws when the second variant metadata write fails after the first variant persists', async () => {
    prismaMock.media.create
      .mockResolvedValueOnce({ id: 'thumbnail-1' })
      .mockRejectedValueOnce(new Error('webp metadata write failed'));

    await expect(processImage(makeJob())).rejects.toThrow('webp metadata write failed');
    expect(prismaMock.media.create).toHaveBeenCalledTimes(2);
  });

  it('re-throws when the S3 variant upload fails', async () => {
    s3UploadMock.mockRejectedValue(new Error('S3 upload failed'));

    await expect(processImage(makeJob())).rejects.toThrow('S3 upload failed');
  });

  it('does not throw when the media row is missing (skips gracefully)', async () => {
    prismaMock.media.findUnique.mockResolvedValue(null);

    await expect(processImage(makeJob())).resolves.toBeUndefined();
  });

  it('does not throw for non-image media (skips gracefully)', async () => {
    prismaMock.media.findUnique.mockResolvedValue(makeMedia({ type: 'DIGITAL_FILE' }));

    await expect(processImage(makeJob())).resolves.toBeUndefined();
  });

  it('completes without throwing on the happy path', async () => {
    await expect(processImage(makeJob())).resolves.toBeUndefined();

    // Both variants created: thumbnail + webp.
    expect(prismaMock.media.create).toHaveBeenCalledTimes(2);
  });
});

describe('deleteMedia ownership (#89)', () => {
  beforeEach(() => {
    s3DeleteMock.mockResolvedValue(undefined);
    prismaMock.media.deleteMany.mockResolvedValue({ count: 1 });
  });

  it('throws NOT_FOUND and deletes nothing when media is missing', async () => {
    prismaMock.media.findUnique.mockResolvedValue(null);

    await expect(deleteMedia(USER_ID, MEDIA_ID)).rejects.toMatchObject({
      code: 'NOT_FOUND',
    });
    expect(s3DeleteMock).not.toHaveBeenCalled();
    expect(prismaMock.media.deleteMany).not.toHaveBeenCalled();
  });

  it('throws FORBIDDEN and never touches storage or DB for a non-owner', async () => {
    await expect(deleteMedia('someone-else', MEDIA_ID)).rejects.toMatchObject({
      code: 'FORBIDDEN',
    });
    expect(s3DeleteMock).not.toHaveBeenCalled();
    expect(prismaMock.media.deleteMany).not.toHaveBeenCalled();
  });

  it('deletes the object, its variants and DB rows for the owner', async () => {
    prismaMock.media.findMany.mockResolvedValue([
      makeMedia({ id: 'variant-1', key: 'images/user-1/file-thumb.webp', variantOfId: MEDIA_ID }),
    ]);

    await deleteMedia(USER_ID, MEDIA_ID);

    expect(s3DeleteMock).toHaveBeenCalledWith('test-bucket', 'images/user-1/file.jpg');
    expect(s3DeleteMock).toHaveBeenCalledWith('test-bucket', 'images/user-1/file-thumb.webp');
    expect(prismaMock.media.deleteMany).toHaveBeenCalledTimes(1);
  });
});
