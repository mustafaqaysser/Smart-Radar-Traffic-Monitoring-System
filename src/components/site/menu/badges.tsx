import { Icon, type IconName } from '@/components/brand/icon';
import type { Allergen, DietaryTag } from '@/lib/db/schema';
import { cn } from '@/lib/utils/cn';

const DIET_ICON: Record<DietaryTag, { icon: IconName; struck?: boolean }> = {
  vegetarian: { icon: 'leaf' },
  vegan: { icon: 'sprout' },
  'gluten-free': { icon: 'gluten', struck: true },
  'dairy-free': { icon: 'milk', struck: true },
  'nut-free': { icon: 'nuts', struck: true },
};

/** Diet marks, spice and allergens for a dish row. Every icon has a text label (visible or for screen readers). */
export function DishMarks({
  dietary,
  allergens,
  spice,
  labels,
  showAllergens = true,
  className,
}: {
  dietary: DietaryTag[];
  allergens: Allergen[];
  spice: number;
  labels: { diet: Record<string, string>; allergen: Record<string, string>; spice: string; /** Full sentence for screen readers, e.g. "Contains gluten and milk". */ contains: string };
  showAllergens?: boolean;
  className?: string;
}) {
  if (!dietary.length && !allergens.length && !spice) return null;
  return (
    <ul className={cn('flex flex-wrap items-center gap-x-3 gap-y-1 text-muted', className)}>
      {dietary.map((d) => (
        <li key={d} className="inline-flex items-center gap-1" title={labels.diet[d]}>
          <Icon name={DIET_ICON[d].icon} struck={DIET_ICON[d].struck} size={16} />
          <span className="t-small">{labels.diet[d]}</span>
        </li>
      ))}
      {spice > 0 ? (
        <li className="inline-flex items-center gap-0.5" title={labels.spice}>
          {Array.from({ length: spice }, (_, i) => (
            <Icon key={i} name="chili" size={15} />
          ))}
          <span className="sr-only">{labels.spice}</span>
        </li>
      ) : null}
      {showAllergens && allergens.length ? (
        <li className="t-small inline-flex flex-wrap items-center gap-1.5">
          <span className="sr-only">{labels.contains}</span>
          {allergens.map((a) => (
            <span key={a} className="inline-flex" title={labels.allergen[a]} aria-hidden="true">
              <Icon name={a as IconName} size={15} />
            </span>
          ))}
        </li>
      ) : null}
    </ul>
  );
}
