# Cronify - Automated Task Scheduler

**Tasks run automatically without opening any browser!**

A multi-user task scheduler that supports:
- 🌐 **Simple Cron** - Ping URLs to keep Render/Heroku/etc apps alive
- 🔥 **Firebase Cron** - Auto delete/backup/cleanup Firebase data

## 🚀 Features

- **User Authentication** - Each user has isolated data
- **Two Task Types:**
  - **URL Ping** - Keep your deployed apps alive by pinging their URLs
  - **Firebase Operations** - Use YOUR OWN Firebase config for operations
- **Real-time Sync** - Tasks update instantly
- **Execution Logs** - Track all task runs
- **Cron Scheduling** - Flexible scheduling with cron expressions

## 📁 Project Structure

```
backend/
├── index.js          # Express server + scheduler
├── package.json
├── .env.example
└── public/           # Frontend (served by Express)
    ├── index.html
    ├── styles.css
    └── app.js
```

## 🔧 Setup Instructions

### Step 1: Create Firebase Project (for Cronify itself)

1. Go to [Firebase Console](https://console.firebase.google.com)
2. Create a new project called "cronify" (or any name)
3. Enable **Authentication** → Email/Password
4. Enable **Realtime Database**
5. Set database rules:
```json
{
  "rules": {
    "users": {
      "$uid": {
        ".read": "$uid === auth.uid",
        ".write": "$uid === auth.uid"
      }
    }
  }
}
```
6. Get your **Web Config** from Project Settings → Your apps → Add web app
7. Download **Service Account JSON** from Project Settings → Service Accounts

### Step 2: Configure Frontend

Edit `backend/public/app.js` and replace `FIREBASE_CONFIG`:
```javascript
const FIREBASE_CONFIG = {
  apiKey: "YOUR_API_KEY",
  authDomain: "YOUR_PROJECT.firebaseapp.com",
  databaseURL: "https://YOUR_PROJECT.firebaseio.com",
  projectId: "YOUR_PROJECT_ID",
  // ... rest of config
};
```

### Step 3: Deploy to Render

1. Push the `backend` folder to GitHub
2. Go to [render.com](https://render.com) → New Web Service
3. Connect your repo
4. Configure:
   - **Root Directory**: `backend` (if you pushed the whole project)
   - **Build Command**: `npm install`
   - **Start Command**: `npm start`
5. Add Environment Variables:
   - `FIREBASE_DATABASE_URL` = Your Cronify Firebase DB URL
   - `FIREBASE_SERVICE_ACCOUNT` = Paste the entire service account JSON
   - `TIMEZONE` = `Asia/Kolkata` (or your timezone)
6. Deploy!

Your site will be live at `https://your-app.onrender.com`

## 📋 How to Use

### Simple Cron (URL Ping)
1. Click **+ New Task**
2. Select **Simple Cron** (🌐)
3. Enter a name (e.g., "Keep Render Alive")
4. Enter your app's URL (e.g., `https://myapp.onrender.com`)
5. Set schedule (e.g., `*/10 * * * *` for every 10 minutes)
6. Enable and save!

### Firebase Cron
1. Click **+ New Task**
2. Select **Firebase Cron** (🔥)
3. Enter task name
4. Paste YOUR Firebase config JSON
5. Paste YOUR Service Account JSON
6. Select action (delete, backup, etc.)
7. Enter target path (e.g., `messages/old`)
8. Set schedule
9. Enable and save!

## 📅 Cron Expression Reference

```
* * * * *
│ │ │ │ └── Day of week (0-7)
│ │ │ └──── Month (1-12)
│ │ └────── Day of month (1-31)
│ └──────── Hour (0-23)
└────────── Minute (0-59)
```

| Expression | Description |
|------------|-------------|
| `*/5 * * * *` | Every 5 minutes |
| `*/10 * * * *` | Every 10 minutes |
| `0 * * * *` | Every hour |
| `0 0 * * *` | Daily at midnight |
| `0 0 * * 0` | Every Sunday |

## 🔥 Firebase Actions

| Action | Description |
|--------|-------------|
| `delete` | Delete ALL data at path |
| `delete_old` | Delete records older than X days |
| `backup` | Copy data to `/backups/` |
| `archive` | Move data to `/archives/` (with delete) |
| `cleanup_null` | Remove null values |

## ⚠️ Render Free Tier Note

Render free tier spins down after 15 mins of inactivity.

**Solution:** Create a Simple Cron task that pings YOUR OWN Cronify URL every 10 minutes!
This keeps Cronify itself alive 24/7.

## 🛡️ Security

- User data is isolated by Firebase Auth UID
- Each user provides their OWN Firebase credentials for Firebase tasks
- Service account JSONs are stored in Firebase
- Never share your service account keys

---

Made with ❤️ for automation
