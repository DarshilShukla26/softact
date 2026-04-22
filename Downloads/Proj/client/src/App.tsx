import { useState } from "react";
import { useTasks } from "./hooks/useTasks";
import TaskForm from "./components/TaskForm";
import TaskItem from "./components/TaskItem";
import FilterTabs, { type Filter } from "./components/FilterTabs";
import "./App.css";

export default function App() {
  const { tasks, loading, error, addTask, toggleTaskById, renameTask, removeTask, clearCompleted } = useTasks();
  const [filter, setFilter] = useState<Filter>("all");

  const counts = {
    all: tasks.length,
    active: tasks.filter((t) => !t.completed).length,
    done: tasks.filter((t) => t.completed).length,
  };

  const visible = tasks.filter((t) => {
    if (filter === "active") return !t.completed;
    if (filter === "done") return t.completed;
    return true;
  });

  return (
    <div className="app">
      <header>
        <div className="badge">
          <span className="badge-dot" />
          Task Manager
        </div>
        <h1>Your Tasks</h1>
        {tasks.length > 0 && (
          <p className="summary">
            {counts.active} task{counts.active !== 1 ? "s" : ""} remaining
          </p>
        )}
      </header>

      <hr className="divider" />

      <TaskForm onAdd={addTask} />

      {tasks.length > 0 && (
        <FilterTabs current={filter} counts={counts} onChange={setFilter} />
      )}

      {loading && <p className="status">Loading tasks…</p>}
      {error && <p className="status error">{error}</p>}

      {!loading && !error && tasks.length === 0 && (
        <p className="status">No tasks yet. Add one above to get started.</p>
      )}

      {!loading && !error && tasks.length > 0 && visible.length === 0 && (
        <p className="status">No {filter} tasks.</p>
      )}

      {visible.length > 0 && (
        <ul className="task-list">
          {visible.map((task) => (
            <TaskItem
              key={task.id}
              task={task}
              onToggle={toggleTaskById}
              onRename={renameTask}
              onDelete={removeTask}
            />
          ))}
        </ul>
      )}

      {tasks.length > 0 && (
        <div className="stats-row">
          <span>
            <b>{counts.active}</b> remaining · <b>{counts.done}</b> done
          </span>
          {counts.done > 0 && (
            <button className="clear-btn" onClick={clearCompleted}>
              Clear completed
            </button>
          )}
        </div>
      )}
    </div>
  );
}
