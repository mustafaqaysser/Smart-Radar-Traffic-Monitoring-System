/**
 * Runs before first paint: marks JS as available, decides whether the first-visit preloader plays (home page
 * only, never under reduced motion) and applies dismissed seasonal notices — so nothing flashes.
 */
export function HeadScript({ nonce, preloader }: { nonce?: string; preloader: boolean }) {
  const code = `(function(){var d=document.documentElement;d.classList.add('js');try{var s=localStorage.getItem('zill_dismissed');if(s){var c=s.split(' ').filter(function(x){return /^[a-z0-9-]+$/.test(x)}).map(function(x){return '.season-banner[data-slug="'+x+'"]{display:none}'}).join('');var st=document.createElement('style');st.textContent=c;document.head.appendChild(st);}${
    preloader
      ? `if(/^\\/(ar|en)\\/?$/.test(location.pathname)&&!localStorage.getItem('zill_seen')&&!matchMedia('(prefers-reduced-motion: reduce)').matches)d.setAttribute('data-preload','1');`
      : ''
  }}catch(e){}})();`;
  return <script nonce={nonce} dangerouslySetInnerHTML={{ __html: code }} />;
}
