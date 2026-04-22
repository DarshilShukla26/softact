import { useState, type FormEvent } from "react";

interface Props {
  onAdd: (title: string) => Promise<void>;
}

export default function TaskForm({ onAdd }: Props) {
  const [title, setTitle] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    if (!title.trim()) return;
    setLoading(true);
    setError("");
    try {
      await onAdd(title.trim());
      setTitle("");
    } catch {
      setError("Failed to add task. Is the server running?");
    } finally {
      setLoading(false);
    }
  }

  return (
    <form className="task-form" onSubmit={handleSubmit}>
      <input
        type="text"
        value={title}
        onChange={(e) => setTitle(e.target.value)}
        placeholder="Add a new task..."
        disabled={loading}
        aria-label="New task title"
      />
      <button type="submit" disabled={loading || !title.trim()}>
        {loading ? "Adding…" : "Add Task"}
      </button>
      {error && <p className="form-error">{error}</p>}
    </form>
  );
}
