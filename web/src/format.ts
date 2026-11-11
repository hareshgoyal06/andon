export const pct = (x: number | undefined) => (x == null ? "—" : `${(x * 100).toFixed(1)}%`);
export const ms = (x: number | undefined) => (x == null || x === 0 ? "—" : `${Math.round(x)} ms`);
export const ago = (iso: string) => {
  const s = Math.max(0, (Date.now() - Date.parse(iso)) / 1000);
  if (s < 60) return `${Math.floor(s)}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
};
export const time = (iso: string) => new Date(iso).toLocaleTimeString([], { hour12: false });
