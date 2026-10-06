import Image from 'next/image';
import type { CSSProperties } from 'react';
import { tr } from '@/lib/i18n/localized';
import type { MediaDTO } from '@/lib/queries/types';
import { cn } from '@/lib/utils/cn';

type Shape = 'rect' | 'arch-4x5' | 'arch-3x4' | 'arch-2x3';

interface MediaImageProps {
  media: MediaDTO;
  locale: string;
  /** Responsive sizes hint (required for good LCP). */
  sizes: string;
  /** Aspect ratio of the frame, e.g. '4/5'. The photograph is cropped around its focal point. */
  ratio?: string;
  shape?: Shape;
  preload?: boolean;
  /** Load immediately without preloading (above-the-fold images that are not the LCP element). */
  eager?: boolean;
  quality?: 55 | 70 | 82;
  className?: string;
  imgClassName?: string;
  /** Decorative images get an empty alt. */
  decorative?: boolean;
  style?: CSSProperties;
}

/** An art-directed photograph: focal-point crop, blur placeholder, AVIF/WebP via next/image. */
export function MediaImage({ media, locale, sizes, ratio, shape = 'rect', preload = false, eager = false, quality = 70, className, imgClassName, decorative, style }: MediaImageProps) {
  const position = `${Math.round(media.focalX * 100)}% ${Math.round(media.focalY * 100)}%`;
  return (
    <div className={cn('relative overflow-hidden bg-surface', shape !== 'rect' && shape, className)} style={{ aspectRatio: ratio ?? `${media.width}/${media.height}`, ...style }}>
      <Image
        src={media.src}
        alt={decorative ? '' : tr(media.alt, locale)}
        fill
        sizes={sizes}
        quality={quality}
        preload={preload}
        loading={preload ? undefined : eager ? 'eager' : 'lazy'}
        placeholder={media.blur ? 'blur' : 'empty'}
        blurDataURL={media.blur ?? undefined}
        className={cn('object-cover', imgClassName)}
        style={{ objectPosition: position }}
      />
    </div>
  );
}
