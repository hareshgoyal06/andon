export function Sparkline({ values, target, width = 160, height = 36 }: { values: number[]; target?: number; width?: number; height?: number }) {
  if (values.length < 2) return <svg width={width} height={height} className="spark" />;
  const all = target ? [...values, target] : values;
  const min = Math.min(...all) * 0.95;
  const max = Math.max(...all) * 1.05;
  const x = (i: number) => (i / (values.length - 1)) * width;
  const y = (v: number) => height - ((v - min) / (max - min || 1)) * height;
  const d = values.map((v, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  return (
    <svg width={width} height={height} className="spark">
      {target && <line x1={0} x2={width} y1={y(target)} y2={y(target)} className="spark-target" />}
      <path d={d} className="spark-line" />
    </svg>
  );
}
