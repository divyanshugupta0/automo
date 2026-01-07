require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const cron = require('node-cron');
const admin = require('firebase-admin');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// Serve static files (frontend)
app.use(express.static(path.join(__dirname, 'public')));

// Initialize Firebase Admin (for Cronify's own database)
let db;
function initFirebase() {
    try {
        const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
        admin.initializeApp({
            credential: admin.credential.cert(serviceAccount),
            databaseURL: process.env.FIREBASE_DATABASE_URL
        });
        db = admin.database();
        console.log('✅ Firebase Admin initialized');
    } catch (e) {
        console.error('❌ Firebase init failed:', e.message);
        process.exit(1);
    }
}
initFirebase();

// Store active cron jobs: { `${userId}_${taskId}`: cronJob }
const activeJobs = new Map();

// =========== API ENDPOINTS ===========

app.get('/api/status', (req, res) => {
    res.json({
        status: 'running',
        service: 'Cronify',
        activeJobs: activeJobs.size,
        time: new Date().toISOString()
    });
});

// Run task immediately
app.post('/api/run-task', async (req, res) => {
    try {
        const { userId, taskId } = req.body;
        if (!userId || !taskId) {
            return res.status(400).json({ success: false, error: 'Missing userId or taskId' });
        }

        // Get task from Firebase
        const taskSnap = await db.ref(`users/${userId}/tasks/${taskId}`).once('value');
        const task = taskSnap.val();
        if (!task) {
            return res.status(404).json({ success: false, error: 'Task not found' });
        }

        const result = await executeTask(userId, task);
        res.json({ success: result.success, result });
    } catch (e) {
        console.error('Run error:', e);
        res.status(500).json({ success: false, error: e.message });
    }
});

// =========== TASK EXECUTION ===========

async function executeTask(userId, task) {
    console.log(`⏰ Executing task: ${task.name} (${task.type})`);
    let result = { success: false, message: '' };

    try {
        // Check if this is a paid task and needs credit deduction
        if (task.isPaid) {
            const planSnap = await db.ref(`users/${userId}/plan`).once('value');
            const plan = planSnap.val() || { credits: 0 };
            if (plan.credits <= 0) {
                result = { success: false, message: 'No credits remaining. Please purchase more.' };
                await logExecution(userId, task, 'failed', result.message);
                return result;
            }
            // Deduct 1 credit
            await db.ref(`users/${userId}/plan/credits`).set(plan.credits - 1);
        }

        if (task.type === 'url') {
            result = await executeUrlPing(task.url);
        } else if (task.type === 'firebase') {
            result = await executeFirebaseTask(task);
        }

        // Log execution
        await logExecution(userId, task, result.success ? 'success' : 'failed', result.message);

        // Update task stats
        await db.ref(`users/${userId}/tasks/${task.id}`).update({
            lastRun: Date.now(),
            runCount: (task.runCount || 0) + 1,
            status: result.success ? 'success' : 'failed'
        });

        console.log(`✅ Task ${task.name}: ${result.message}`);
    } catch (e) {
        result = { success: false, message: e.message };
        await logExecution(userId, task, 'failed', e.message);
        console.error(`❌ Task ${task.name} failed:`, e.message);
    }

    return result;
}

async function executeUrlPing(url) {
    try {
        const response = await fetch(url, { method: 'GET', signal: AbortSignal.timeout(30000) });
        return {
            success: true,
            message: `Pinged successfully (${response.status})`
        };
    } catch (e) {
        return { success: false, message: `Ping failed: ${e.message}` };
    }
}

async function executeFirebaseTask(task) {
    let userApp;

    try {
        const config = JSON.parse(task.firebaseConfig);
        const serviceAccount = JSON.parse(task.serviceAccount);

        const appName = `user_${Date.now()}_${Math.random().toString(36).slice(2)}`;
        userApp = admin.initializeApp({
            credential: admin.credential.cert(serviceAccount),
            databaseURL: config.databaseURL
        }, appName);
        const userDb = userApp.database();

        let result;
        const targetRef = userDb.ref(task.targetPath);

        switch (task.action) {
            case 'delete':
                const snap = await targetRef.once('value');
                const count = snap.numChildren();
                await targetRef.remove();
                result = { success: true, message: `Deleted ${count} items at ${task.targetPath}` };
                break;

            case 'delete_old':
                const cutoff = Date.now() - (task.olderThanDays * 24 * 60 * 60 * 1000);
                const oldSnap = await targetRef.orderByChild('timestamp').endAt(cutoff).once('value');
                const updates = {};
                let delCount = 0;
                oldSnap.forEach(child => { updates[child.key] = null; delCount++; });
                if (delCount > 0) await targetRef.update(updates);
                result = { success: true, message: `Deleted ${delCount} old items` };
                break;

            case 'backup':
                const dataSnap = await targetRef.once('value');
                const data = dataSnap.val();
                if (data) {
                    const backupPath = `backups/${task.targetPath.replace(/\//g, '_')}_${Date.now()}`;
                    await userDb.ref(backupPath).set({ data, backedUpAt: Date.now() });
                    result = { success: true, message: `Backed up to ${backupPath}` };
                } else {
                    result = { success: true, message: 'No data to backup' };
                }
                break;

            case 'archive':
                const archiveSnap = await targetRef.once('value');
                const archiveData = archiveSnap.val();
                if (archiveData) {
                    const archivePath = `archives/${task.targetPath.replace(/\//g, '_')}_${Date.now()}`;
                    await userDb.ref(archivePath).set({ data: archiveData, archivedAt: Date.now() });
                    await targetRef.remove();
                    result = { success: true, message: `Archived to ${archivePath}` };
                } else {
                    result = { success: true, message: 'No data to archive' };
                }
                break;

            case 'cleanup_null':
                const cleanSnap = await targetRef.once('value');
                const cleanData = cleanSnap.val();
                if (cleanData && typeof cleanData === 'object') {
                    const cleanUpdates = {};
                    let cleanCount = 0;
                    for (const [key, val] of Object.entries(cleanData)) {
                        if (val === null || val === undefined) {
                            cleanUpdates[key] = null;
                            cleanCount++;
                        }
                    }
                    if (cleanCount > 0) await targetRef.update(cleanUpdates);
                    result = { success: true, message: `Cleaned ${cleanCount} null values` };
                } else {
                    result = { success: true, message: 'No nulls found' };
                }
                break;

            default:
                result = { success: false, message: `Unknown action: ${task.action}` };
        }

        return result;
    } catch (e) {
        return { success: false, message: e.message };
    } finally {
        if (userApp) {
            try { await userApp.delete(); } catch (e) { }
        }
    }
}

async function logExecution(userId, task, status, message) {
    await db.ref(`users/${userId}/logs`).push({
        taskId: task.id,
        taskName: task.name,
        type: task.type,
        status,
        message,
        timestamp: Date.now()
    });
}

// =========== SCHEDULING ===========

function scheduleTask(userId, task) {
    const jobKey = `${userId}_${task.id}`;

    cancelJob(jobKey);

    if (!task.schedule || !cron.validate(task.schedule)) {
        console.warn(`Invalid cron for task ${task.id}: ${task.schedule}`);
        return;
    }

    const job = cron.schedule(task.schedule, async () => {
        // Reload task to get latest data
        const snap = await db.ref(`users/${userId}/tasks/${task.id}`).once('value');
        const currentTask = snap.val();
        if (currentTask && currentTask.enabled) {
            await executeTask(userId, currentTask);
        }
    }, { scheduled: true, timezone: process.env.TIMEZONE || 'UTC' });

    activeJobs.set(jobKey, job);
    console.log(`📅 Scheduled: ${task.name} (${task.schedule})`);
}

function cancelJob(jobKey) {
    const job = activeJobs.get(jobKey);
    if (job) {
        job.stop();
        activeJobs.delete(jobKey);
    }
}

// Load all tasks on startup
async function loadAllTasks() {
    console.log('Loading all user tasks...');
    try {
        const usersSnap = await db.ref('users').once('value');
        const users = usersSnap.val() || {};

        let count = 0;
        for (const [userId, userData] of Object.entries(users)) {
            const tasks = userData.tasks || {};
            for (const [taskId, task] of Object.entries(tasks)) {
                if (task.enabled) {
                    scheduleTask(userId, task);
                    count++;
                }
            }
        }
        console.log(`✅ Loaded ${count} active tasks`);
    } catch (e) {
        console.error('Error loading tasks:', e);
    }
}

// Watch for task changes in real-time
function watchTaskChanges() {
    db.ref('users').on('child_changed', async (snap) => {
        const userId = snap.key;
        const userData = snap.val() || {};
        const tasks = userData.tasks || {};

        // Update scheduled jobs for this user
        for (const [taskId, task] of Object.entries(tasks)) {
            const jobKey = `${userId}_${taskId}`;
            if (task.enabled) {
                scheduleTask(userId, task);
            } else {
                cancelJob(jobKey);
            }
        }
    });

    // Handle deleted users/tasks
    db.ref('users').on('child_removed', (snap) => {
        const userId = snap.key;
        // Cancel all jobs for this user
        for (const [jobKey] of activeJobs) {
            if (jobKey.startsWith(`${userId}_`)) {
                cancelJob(jobKey);
            }
        }
    });

    console.log('👀 Watching for task changes...');
}

// Serve frontend for all other routes
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
app.listen(PORT, async () => {
    console.log(`🚀 Cronify running on port ${PORT}`);
    console.log(`📍 http://localhost:${PORT}`);
    await loadAllTasks();
    watchTaskChanges();
});
