interface StarsDisplayProps {
  rating: number;
}

export function StarsDisplay({ rating }: StarsDisplayProps) {
  return (
    <span className="stars-display">
      {[1, 2, 3, 4, 5].map((s) => {
        const fill = Math.min(1, Math.max(0, rating - (s - 1)));
        return (
          <span key={s} className="star-wrapper">
            <span className="star">★</span>
            <span className="star filled" style={{ width: `${fill * 100}%` }}>★</span>
          </span>
        );
      })}
    </span>
  );
}

interface StarsInputProps {
  value: number;
  hoverValue: number;
  onHover: (value: number) => void;
  onSelect: (value: number) => void;
}

export function StarsInput({ value, hoverValue, onHover, onSelect }: StarsInputProps) {
  const display = hoverValue || value;

  return (
    <span className="stars-input">
      {[1, 2, 3, 4, 5].map((s) => {
        const fill = Math.min(1, Math.max(0, display - (s - 1)));
        return (
          <span key={s} className="star-input-wrapper">
            <span className="star">★</span>
            <span className="star filled" style={{ width: `${fill * 100}%` }}>★</span>
            {[0.25, 0.5, 0.75, 1.0].map((q) => (
              <span
                key={q}
                className="star-quarter-zone"
                style={{ left: `${(q - 0.25) * 100}%`, width: '25%' }}
                onMouseEnter={() => onHover(s - 1 + q)}
                onMouseLeave={() => onHover(0)}
                onClick={() => onSelect(s - 1 + q)}
              />
            ))}
          </span>
        );
      })}
      {display > 0 && <span className="stars-value">{display.toFixed(2).replace(/\.?0+$/, '')}</span>}
    </span>
  );
}
