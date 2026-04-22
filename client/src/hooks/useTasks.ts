import { useEffect, useState } from "react";
import type { Task } from "../types";
import { fetchTasks, createTask, patchTask, deleteTask } from "../api";

export function useTasks() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    fetchTasks()
      .then(setTasks)
      .catch(() => setError("Could not connect to server. Make sure the backend is running on port 3001."))
      .finally(() => setLoading(false));
  }, []);

  async function addTask(title: string) {
    const optimistic: Task = {
      id: `optimistic-${Date.now()}`,
      title,
      completed: false,
      createdAt: new Date().toISOString(),
    };
    setTasks((prev) => [...prev, optimistic]);
    try {
      const saved = await createTask(title);
      setTasks((prev) => prev.map((t) => (t.id === optimistic.id ? saved : t)));
    } catch {
      setTasks((prev) => prev.filter((t) => t.id !== optimistic.id));
      throw new Error("Failed to add task");
    }
  }

  async function toggleTaskById(id: string, completed: boolean) {
    setTasks((prev) => prev.map((t) => (t.id === id ? { ...t, completed } : t)));
    try {
      const updated = await patchTask(id, { completed });
      setTasks((prev) => prev.map((t) => (t.id === id ? updated : t)));
    } catch {
      setTasks((prev) => prev.map((t) => (t.id === id ? { ...t, completed: !completed } : t)));
    }
  }

  async function renameTask(id: string, title: string) {
    const prev_title = tasks.find((t) => t.id === id)?.title ?? "";
    setTasks((prev) => prev.map((t) => (t.id === id ? { ...t, title } : t)));
    try {
      const updated = await patchTask(id, { title });
      setTasks((prev) => prev.map((t) => (t.id === id ? updated : t)));
    } catch {
      setTasks((prev) => prev.map((t) => (t.id === id ? { ...t, title: prev_title } : t)));
    }
  }

  async function removeTask(id: string) {
    const snapshot = tasks.find((t) => t.id === id);
    setTasks((prev) => prev.filter((t) => t.id !== id));
    try {
      await deleteTask(id);
    } catch {
      if (snapshot) setTasks((prev) => [...prev, snapshot]);
    }
  }

  async function clearCompleted() {
    const completed = tasks.filter((t) => t.completed);
    setTasks((prev) => prev.filter((t) => !t.completed));
    try {
      await Promise.all(completed.map((t) => deleteTask(t.id)));
    } catch {
      setTasks((prev) => [...prev, ...completed]);
    }
  }

  return { tasks, loading, error, addTask, toggleTaskById, renameTask, removeTask, clearCompleted };
}
