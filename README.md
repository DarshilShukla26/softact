# Task Manager — SOFTACT Take-Home Assignment

A full-stack task management app built with **React + TypeScript** (frontend) and **Node.js + Express** (backend).

## Running Locally

### Prerequisites
- Node.js ≥ 18
- npm

### 1. Start the backend

```bash
cd server
npm install
npm start
```

The API runs at `http://localhost:3001`.

### 2. Start the frontend (separate terminal)

```bash
cd client
npm install
npm run dev
```

The app opens at `http://localhost:5173`.

---

## Features

- Add tasks via the input form
- Toggle completion with a checkbox
- **Double-click** any task title to edit it inline (Enter to save, Escape to cancel)
- Delete individual tasks
- Filter by **All / Active / Done**
- Clear all completed tasks at once
- Data persists across page refreshes via a JSON file on the server

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/tasks` | List all tasks |
| POST | `/tasks` | Create a task — body: `{ title: string }` |
| PATCH | `/tasks/:id` | Update a task — body: `{ title?: string, completed?: boolean }` |
| DELETE | `/tasks/:id` | Delete a task |

---

## Project Structure

```
Proj/
├── server/
│   ├── index.js        # Express API (GET, POST, PATCH, DELETE)
│   └── tasks.json      # Auto-created on first task; persists data
└── client/
    └── src/
        ├── api.ts                    # Typed fetch helpers for all endpoints
        ├── types.ts                  # Shared Task interface
        ├── App.tsx                   # Root component — filter state + layout
        ├── hooks/
        │   └── useTasks.ts           # All server communication + task state
        └── components/
            ├── TaskForm.tsx          # Add-task input with loading/error states
            ├── TaskItem.tsx          # Task row — checkbox, inline edit, delete
            └── FilterTabs.tsx        # All / Active / Done filter tabs
```

---

## Decisions & Trade-offs

**Data persistence — JSON file vs. database**
I used a JSON file (`tasks.json`) as the data store. This keeps the project dependency-free and easy to run locally. In production I'd swap in SQLite or Postgres with proper migrations.

**IDs — `Date.now()` as string**
Simple and collision-resistant for a single-server demo. A UUID library would be the right choice in production.

**`useTasks` custom hook**
All server communication and task state live in one hook, keeping `App.tsx` focused purely on layout and filter logic. This separation also makes the data layer straightforward to unit-test in isolation.

**Optimistic UI updates**
All mutations (add, toggle, rename, delete) update local state immediately and silently revert if the server returns an error. This makes the app feel instant without sacrificing consistency.

**Inline edit via PATCH**
Double-clicking a task title opens an in-place input. On commit it fires `PATCH /tasks/:id` with the new title, demonstrating the full use of the PATCH endpoint beyond just toggling completion.

**TypeScript on the frontend only**
The backend is plain JavaScript to keep it concise. In a real project I'd type the backend too, or share types via a monorepo package.
