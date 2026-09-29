import type { SVGProps } from 'react';
import { directionalIcons, iconPaths, type IconName } from './icon-paths';

export type { IconName };

interface IconProps extends Omit<SVGProps<SVGSVGElement>, 'name'> {
  name: IconName;
  /** Pixel size (width and height). Defaults to 20. */
  size?: number;
  /** Accessible label. Without it the icon is decorative (aria-hidden). */
  label?: string;
  /** Draw a diagonal strike (e.g. gluten-free, dairy-free). */
  struck?: boolean;
}

export function Icon({ name, size = 20, label, struck = false, className, ...rest }: IconProps) {
  const mirror = directionalIcons.has(name);
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 24 24"
      width={size}
      height={size}
      fill="none"
      stroke="currentColor"
      strokeWidth={1.5}
      strokeLinecap="square"
      strokeLinejoin="miter"
      role={label ? 'img' : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : true}
      focusable="false"
      className={[mirror ? 'flip-rtl' : '', 'shrink-0', className].filter(Boolean).join(' ')}
      {...rest}
    >
      <path d={iconPaths[name]} />
      {struck ? <path d="M4 20 20 4" /> : null}
    </svg>
  );
}
