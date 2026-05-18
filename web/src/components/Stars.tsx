interface StarsDisplayProps {
  rating: number;
}

export function StarsDisplay({ rating }: StarsDisplayProps) {
  return (
    <span className="stars-display" role="img" aria-label={`${rating} out of 5 stars`}>
      {[1, 2, 3, 4, 5].map((s) => {
        const fill = Math.min(1, Math.max(0, rating - (s - 1)));
        return (
          <span key={s} className="star-wrapper">
            <span className="star">★</span>
            <span className="star filled" style={{ width: `${fill * 100}%` }}>
              ★
            </span>
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
    <fieldset className="stars-input" aria-label="Star rating selector">
      {[1, 2, 3, 4, 5].map((s) => {
        const fill = Math.min(1, Math.max(0, display - (s - 1)));
        return (
          <span key={s} className="star-input-wrapper">
            <span className="star">★</span>
            <span className="star filled" style={{ width: `${fill * 100}%` }}>
              ★
            </span>
            {[0.25, 0.5, 0.75, 1.0].map((q) => (
              <button
                key={q}
                type="button"
                className="star-quarter-zone"
                tabIndex={0}
                aria-label={`Rate ${s - 1 + q} stars`}
                style={{ left: `${(q - 0.25) * 100}%`, width: "25%" }}
                onMouseEnter={() => onHover(s - 1 + q)}
                onMouseLeave={() => onHover(0)}
                onClick={() => onSelect(s - 1 + q)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    onSelect(s - 1 + q);
                  }
                }}
              />
            ))}
          </span>
        );
      })}
      {display > 0 && (
        <span className="stars-value">{display.toFixed(2).replace(/\.?0+$/, "")}</span>
      )}
    </fieldset>
  );
}
