'use client';

/**
 * The new-order chime, synthesised with the Web Audio API (no audio file to load). Browsers only allow sound
 * after a person has interacted with the page, so the context is created or resumed from a click or key press.
 */
let context: AudioContext | null = null;

export function audioReady(): boolean {
  return context?.state === 'running';
}

/** Call from a user gesture (a click, a key press). Returns whether sound can now play. */
export async function unlockAudio(): Promise<boolean> {
  try {
    context ??= new AudioContext();
    if (context.state === 'suspended') await context.resume();
    return context.state === 'running';
  } catch {
    return false;
  }
}

/** Two soft bell tones, rising — distinct from browser and phone notification sounds. */
export function chime(times = 1): void {
  const ctx = context;
  if (!ctx || ctx.state !== 'running') return;
  for (let r = 0; r < times; r++) {
    [659.25, 987.77].forEach((freq, i) => {
      const start = ctx.currentTime + r * 0.9 + i * 0.18;
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = 'sine';
      osc.frequency.value = freq;
      gain.gain.setValueAtTime(0.0001, start);
      gain.gain.exponentialRampToValueAtTime(0.32, start + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.0001, start + 0.75);
      osc.connect(gain).connect(ctx.destination);
      osc.start(start);
      osc.stop(start + 0.8);
    });
  }
}
