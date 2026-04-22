export type Filter = "all" | "active" | "done";

interface Props {
  current: Filter;
  counts: { all: number; active: number; done: number };
  onChange: (f: Filter) => void;
}

const TABS: { key: Filter; label: string }[] = [
  { key: "all", label: "All" },
  { key: "active", label: "Active" },
  { key: "done", label: "Done" },
];

export default function FilterTabs({ current, counts, onChange }: Props) {
  return (
    <div className="filter-tabs">
      {TABS.map(({ key, label }) => (
        <button
          key={key}
          className={`filter-tab ${current === key ? "active" : ""}`}
          onClick={() => onChange(key)}
        >
          {label}
          <span className="tab-count">{counts[key]}</span>
        </button>
      ))}
    </div>
  );
}
