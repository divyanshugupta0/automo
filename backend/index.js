require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const cron = require('node-cron');
const admin = require('firebase-admin');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json({ limit: '10mb' }));

console.log('--- BACKEND SERVER STARTED (V4: QUIET MODE) ---');

// Serve static files (frontend)
app.use(express.static(path.join(__dirname, 'public')));

// Initialize Firebase Admin (for Joblix's own database)
let db;
function initFirebase() {
    try {
        const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
        // Check if already initialized to prevent errors during hot reload
        if (!admin.apps.length) {
            admin.initializeApp({
                credential: admin.credential.cert(serviceAccount),
                databaseURL: process.env.FIREBASE_DATABASE_URL
            });
        }
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

// =========== ENCRYPTION HELPERS (ASYNC) ===========
const ENCRYPTION_ALGORITHM = 'aes-256-cbc';
const IV_LENGTH = 16;
const util = require('util');
const scryptAsync = util.promisify(crypto.scrypt);

async function encryptText(text, secret) {
    if (!text) return null;
    try {
        // Derive key from secret asynchronously
        const keyBuffer = await scryptAsync(secret, 'salt', 32);
        const iv = crypto.randomBytes(IV_LENGTH);
        const cipher = crypto.createCipheriv(ENCRYPTION_ALGORITHM, keyBuffer, iv);
        let encrypted = cipher.update(text);
        encrypted = Buffer.concat([encrypted, cipher.final()]);
        return iv.toString('hex') + ':' + encrypted.toString('hex');
    } catch (e) {
        console.error('Encryption failed:', e);
        throw new Error('Encryption failed');
    }
}

async function decryptText(text, secret) {
    if (!text) return null;
    try {
        const textParts = text.split(':');
        if (textParts.length < 2) throw new Error('Invalid format');
        const iv = Buffer.from(textParts.shift(), 'hex');
        const encryptedText = Buffer.from(textParts.join(':'), 'hex');
        const keyBuffer = await scryptAsync(secret, 'salt', 32);
        const decipher = crypto.createDecipheriv(ENCRYPTION_ALGORITHM, keyBuffer, iv);
        let decrypted = decipher.update(encryptedText);
        decrypted = Buffer.concat([decrypted, decipher.final()]);
        return decrypted.toString();
    } catch (e) {
        if (e.message === 'Invalid format') throw e; // Re-throw simple error
        // console.error('Decryption failed:', e); // Squelch noise
        throw new Error('Decryption failed or invalid key');
    }
}

// =========== AUTH MIDDLEWARE ===========
async function validateApiKey(req, res, next) {
    const apiKey = req.headers['x-api-key'];
    if (!apiKey) {
        return res.status(401).json({ success: false, error: 'Missing x-api-key header' });
    }

    try {
        // Look up user by API key
        // We expect apiKeys/{key} = userId
        const keySnap = await db.ref(`apiKeys/${apiKey}`).once('value');
        const userId = keySnap.val();

        if (!userId) {
            return res.status(401).json({ success: false, error: 'Invalid API Key' });
        }

        req.userId = userId;
        next();
    } catch (e) {
        res.status(500).json({ success: false, error: 'Auth Error' });
    }
}

// =========== API ENDPOINTS ===========

app.get('/api/status', (req, res) => {
    res.json({
        status: 'running',
        service: 'Joblix',
        activeJobs: activeJobs.size,
        time: new Date().toISOString()
    });
});

// Rate Limiter (Memory-based for speed)
const rateLimitMap = new Map();
const RATE_LIMIT_WINDOW = 60 * 1000; // 1 minute
const RATE_LIMIT_MAX = 60; // 60 requests per minute

function checkRateLimit(req, res, next) {
    const key = req.userId; // Limit by User (API Key owner)
    const now = Date.now();

    if (!rateLimitMap.has(key)) {
        rateLimitMap.set(key, { count: 1, resetTime: now + RATE_LIMIT_WINDOW });
        return next();
    }

    const data = rateLimitMap.get(key);

    if (now > data.resetTime) {
        // Window expired, reset
        data.count = 1;
        data.resetTime = now + RATE_LIMIT_WINDOW;
        return next();
    }

    if (data.count >= RATE_LIMIT_MAX) {
        return res.status(429).json({
            success: false,
            error: 'Too many requests. Limit is 60 per minute.',
            retryAfter: Math.ceil((data.resetTime - now) / 1000)
        });
    }

    data.count++;
    next();
}

// Generate API Key (Call this from Frontend)
app.post('/api/generate-key', async (req, res) => {
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ error: 'UserId required' });

    try {
        const newKey = crypto.randomBytes(24).toString('hex');
        const secret = crypto.randomBytes(32).toString('hex'); // Used for encryption salt

        // Remove old key for this user if exists (optional cleanup)
        const oldKeySnap = await db.ref(`users/${userId}/apiKey`).once('value');
        if (oldKeySnap.exists()) {
            await db.ref(`apiKeys/${oldKeySnap.val()}`).remove();
        }

        // Set updates
        const updates = {};
        updates[`users/${userId}/apiKey`] = newKey;
        updates[`users/${userId}/encryptionSecret`] = secret;
        updates[`apiKeys/${newKey}`] = userId;

        await db.ref().update(updates);

        res.json({ success: true, apiKey: newKey });
    } catch (e) {
        console.error('Generate Key Error:', e);
        res.status(500).json({ success: false, error: e.message });
    }
});



// Check API Balance Helpers
async function checkAndDeductCredit(userId, cost = 1) {
    const planSnap = await db.ref(`users/${userId}/plan`).once('value');
    const plan = planSnap.val() || { credits: 0 };

    // Free users might have a separate small buffer or just use credits if we gave them some.
    // Assuming for API requests we strictly require credits or tracking

    // For now, let's just track it in a separate 'apiUsage' counter effectively billing 1 credit per request
    // or simply checking if they have credits.

    if (plan.credits < cost) {
        throw new Error('Insufficient credits. Please recharge.');
    }

    await db.ref(`users/${userId}/plan/credits`).set(plan.credits - cost);
    // Track usage stats
    await db.ref(`users/${userId}/stats/apiRequests`).transaction(count => (count || 0) + 1);

    return true;
}

// Submit Encryption Request
// Submit Encryption Request
// Submit Encryption Request
app.post('/api/service/encrypt', validateApiKey, checkRateLimit, async (req, res) => {
    const { text } = req.body;
    if (!text) return res.status(400).json({ error: 'Text is required' });

    try {
        const origin = req.get('origin') || req.get('referer') || 'Direct/Unknown';

        // OPTIMIZATION: Fetch all needed User Data in PARALLEL for Real-time speed
        // 1. Tasks (for Origin Check)
        // 2. Secret (for Execution)
        // 3. Plan (for Billing check - though separate write is still needed)

        const [tasksSnap, secretSnap, planSnap] = await Promise.all([
            db.ref(`users/${req.userId}/tasks`).once('value'),
            db.ref(`users/${req.userId}/encryptionSecret`).once('value'),
            db.ref(`users/${req.userId}/plan`).once('value')
        ]);

        const tasks = tasksSnap.val() || {};
        const secret = secretSnap.val();
        const plan = planSnap.val() || { credits: 0 };

        // --- 1. Validate Origin against Tasks ---
        const encTasks = Object.values(tasks).filter(t => t.type === 'encryption_worker');

        // If user has defined specific domains, enforce them
        const domainTasks = encTasks.filter(t => t.url && t.url.trim().length > 0);

        let authorized = true;
        let matchedTaskName = 'General API';

        if (domainTasks.length > 0) {
            // Helper to extract hostname (remove protocol, path, port)
            const getHost = (url) => url.replace(/^https?:\/\//, '').split('/')[0].split(':')[0].toLowerCase();
            const requestHost = getHost(origin);

            const match = domainTasks.find(t => {
                const allowedHost = getHost(t.url);
                // Allow exact match OR subdomain (must start with dot)
                // e.g. allowed: "google.com" -> matches "google.com", "mail.google.com"
                // but NOT "fakegoogle.com"
                return requestHost === allowedHost || requestHost.endsWith('.' + allowedHost);
            });

            if (!match) {
                // Return 403 if browser request doesn't match allowlist
                if (origin !== 'Direct/Unknown') {
                    return res.status(403).json({ error: `Origin '${origin}' not authorized. Allowed: ${domainTasks.map(t => getHost(t.url)).join(', ')}` });
                }
            } else {
                matchedTaskName = match.name;
                if (!match.enabled) return res.status(403).json({ error: `Encryption disabled for '${matchedTaskName}'` });
            }
        }

        // --- Billing Check (optimization: check memory before DB write) ---
        if (plan.credits < 1) {
            return res.status(402).json({ success: false, error: 'Insufficient credits. Please recharge.' });
        }

        // --- ULTRA-LOW LATENCY MODE ---
        // 1. Calculate Result Immediately
        // 2. Send Response
        // 3. Save to DB in Background

        const requestId = db.ref().push().key;

        // If we have secret, try strictly in memory
        if (secret) {
            try {
                const encrypted = await encryptText(text, secret);

                // SEND RESPONSE IMMEDIATELY
                res.json({
                    success: true,
                    requestId: requestId,
                    message: 'Encryption completed immediately.',
                    status: 'completed',
                    result: encrypted,
                    processedBy: 'Immediate (Optimized)'
                });

                // BACKGROUND: Save History & Billing
                (async () => {
                    try {
                        // Billing & Stats
                        if (origin !== 'Direct/Unknown') {
                            const safeOrigin = origin.replace(/[.#$/[\]]/g, '_');
                            await db.ref(`users/${req.userId}/stats/sites/${safeOrigin}`).transaction(count => (count || 0) + 1);
                        }
                        await db.ref(`users/${req.userId}/plan/credits`).set(plan.credits - 1);
                        await db.ref(`users/${req.userId}/stats/apiRequests`).transaction(count => (count || 0) + 1);

                        // Save Log
                        await db.ref(`users/${req.userId}/encryption_queue/${requestId}`).set({
                            action: 'encrypt',
                            status: 'completed',
                            text: text,
                            result: encrypted,
                            createdAt: Date.now(),
                            processedAt: Date.now(),
                            origin: origin,
                            processedBy: 'Immediate (Optimized)'
                        });
                    } catch (bgErr) {
                        console.error('Background Save Error:', bgErr);
                    }
                })();

                return; // End request here

            } catch (err) {
                console.error('Immediate encryption error:', err);
                // Fallthrough to queue logic below
            }
        }

        // --- FALLBACK (Queued) ---
        // Only if immediate failed (e.g. no secret or error)

        // Save pending to DB (Must await this so worker can find it)
        let resultData = {
            action: 'encrypt',
            status: 'pending',
            text: text,
            createdAt: Date.now(),
            origin: origin,
            processedBy: 'Pending'
        };

        await db.ref(`users/${req.userId}/encryption_queue/${requestId}`).set(resultData);

        // Trigger Worker via Drain Logic
        if (encTasks.length > 0) {
            // Fire and forget drain
            setTimeout(() => drainEncryptionQueue(req.userId, tasks), 50);
        }

        res.json({
            success: true,
            requestId,
            message: 'Encryption queued (Worker triggered).',
            status: resultData.status,
            result: resultData.result,
            processedBy: resultData.processedBy
        });

    } catch (e) {
        res.status(402).json({ success: false, error: e.message }); // 402 Payment Required
    }
});

// Submit Decryption Request
// Submit Decryption Request
// Submit Decryption Request
app.post('/api/service/decrypt', validateApiKey, checkRateLimit, async (req, res) => {
    const { text } = req.body; // Encrypted string
    if (!text) return res.status(400).json({ error: 'Text is required' });

    try {
        // Billing: Deduct 1 Credit per request
        // This is now handled in the ultra-low latency block or the fallback queue logic.
        // await checkAndDeductCredit(req.userId, 1); // Removed from here

        const requestId = db.ref().push().key;

        // Fetch user data and tasks in parallel
        const [userSnap, tasksSnap] = await Promise.all([
            db.ref(`users/${req.userId}`).once('value'),
            db.ref(`users/${req.userId}/tasks`).once('value')
        ]);
        const userData = userSnap.val();
        const tasks = tasksSnap.val() || {};
        const encTasks = Object.values(tasks).filter(t => t.type === 'encryption_worker');
        const secret = userData ? userData.encryptionSecret : null;
        const origin = req.get('origin') || req.get('referer') || 'Direct/Unknown';


        if (secret) {
            try {
                const decrypted = await decryptText(text, secret);

                res.json({
                    success: true,
                    requestId,
                    message: 'Decryption completed immediately.',
                    status: 'completed',
                    result: decrypted,
                    processedBy: 'Immediate (Optimized)'
                });

                // BACKGROUND: Save History & Billing
                (async () => {
                    try {
                        if (origin !== 'Direct/Unknown') {
                            const safeOrigin = origin.replace(/[.#$/[\]]/g, '_');
                            await db.ref(`users/${req.userId}/stats/sites/${safeOrigin}`).transaction(count => (count || 0) + 1);
                        }
                        await db.ref(`users/${req.userId}/plan/credits`).transaction(currentCredits => (currentCredits || 0) - 1);
                        await db.ref(`users/${req.userId}/stats/apiRequests`).transaction(count => (count || 0) + 1);

                        await db.ref(`users/${req.userId}/encryption_queue/${requestId}`).set({
                            action: 'decrypt',
                            status: 'completed',
                            text: text,
                            result: decrypted,
                            createdAt: Date.now(),
                            processedAt: Date.now(),
                            origin: origin,
                            processedBy: 'Immediate (Optimized)'
                        });
                    } catch (bgErr) { console.error('Background Decryption Save Error:', bgErr); }
                })();

                return;
                return;
            } catch (err) {
                // Squelch expected errors
                console.warn(`⚠️ Immediate decryption failed: ${err.message}`);
            }
        }

        // Fallback to queued processing if immediate failed or no secret
        // First, deduct credit for queued request
        await checkAndDeductCredit(req.userId, 1);

        let resultData = {
            action: 'decrypt',
            status: 'pending',
            text: text,
            createdAt: Date.now(),
            processedBy: 'Pending'
        };

        await db.ref(`users/${req.userId}/encryption_queue/${requestId}`).set(resultData);

        // Trigger Worker via Drain Logic
        if (encTasks.length > 0) {
            setTimeout(() => drainEncryptionQueue(req.userId, tasks), 50);
        }

        res.json({
            success: true,
            requestId,
            message: 'Decryption queued (Worker triggered).',
            status: resultData.status,
            result: resultData.result,
            error: resultData.error,
            processedBy: resultData.processedBy
        });

    } catch (e) {
        res.status(402).json({ success: false, error: e.message });
    }
});

// Submit Decryption Request
// (Handled above in block)

// Get Result
app.post('/api/service/result', validateApiKey, async (req, res) => {
    const { requestId } = req.body;
    if (!requestId) return res.status(400).json({ error: 'RequestId required' });

    try {
        const snap = await db.ref(`users/${req.userId}/encryption_queue/${requestId}`).once('value');
        const data = snap.val();

        if (!data) return res.status(404).json({ error: 'Request not found' });

        res.json({
            success: true,
            status: data.status,
            result: data.result,
            error: data.error,
            processedBy: data.processedBy || 'Immediate/System'
        });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
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
    // console.log(`⏰ Executing task: ${task.name} (${task.type})`);
    let result = { success: false, message: '' };
    const startTime = Date.now();

    try {
        // Check if this is a paid task and needs credit deduction
        // EXCEPTION: Encryption Workers are system tasks and should not cost credits to run (requests are billed individually)
        if (task.isPaid && task.type !== 'encryption_worker') {
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
        } else if (task.type === 'encryption_worker') {
            result = await executeEncryptionTask(userId, task);
        }

        // SUPPRESS NOISE: Don't log "No pending items" success messages
        if (result.success && result.message && result.message.includes('No pending encryption items')) {
            // Do not log, and DO NOT update stats (treat as if it didn't run)
            // console.log(`ℹ️ Task ${task.name}: No pending items (Log & Stats suppressed)`);
        } else {
            // Log execution
            const duration = Date.now() - startTime;
            let logMsg = result.message;
            if (!logMsg || logMsg.trim() === '') {
                logMsg = result.success ? 'Operation successful (no detail)' : 'Error: No error message provided by task';
            }

            await logExecution(userId, task, result.success ? 'success' : 'failed', logMsg, duration);

            // Update task stats ONLY if meaningful work happened
            await db.ref(`users/${userId}/tasks/${task.id}`).update({
                lastRun: Date.now(),
                runCount: (task.runCount || 0) + 1,
                status: result.success ? 'success' : 'failed'
            });
        }

        // console.log(`✅ Task ${task.name}: ${result.message}`);
    } catch (e) {
        result = { success: false, message: e.message || 'Unknown exception v2' };
        try {
            await logExecution(userId, task, 'failed', result.message);
        } catch (logErr) { console.error('Logging failed', logErr); }
        console.error(`❌ Task ${task.name} failed:`, result.message);
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

async function executeEncryptionTask(userId, task) {
    try {
        // Fetch User's Encryption Secret
        const userSnap = await db.ref(`users/${userId}/encryptionSecret`).once('value');
        let secret = userSnap.val();

        if (!secret) {
            return { success: false, message: 'No encryption secret found. Please generate an API Key first.' };
        }

        // Fetch Queue
        const queueRef = db.ref(`users/${userId}/encryption_queue`);
        // Limit to 50 items per run to prevent timeout
        const queueSnap = await queueRef.orderByChild('status').equalTo('pending').limitToFirst(50).once('value');
        const queue = queueSnap.val();

        if (!queue) {
            return { success: true, message: 'No pending encryption items' };
        }

        let processed = 0;
        let errors = 0;

        // Process each item one by one with transaction to avoid race conditions
        for (const [key, item] of Object.entries(queue)) {
            // Transaction to claim the task
            const itemRef = queueRef.child(key);
            let claimed = false;

            await itemRef.transaction((current) => {
                if (current && current.status === 'pending') {
                    // Mark as processing so others don't pick it
                    current.status = 'processing';
                    return current;
                }
                return; // Abort if not pending (already picked or deleted)
            }, (error, committed, snapshot) => {
                if (error) {
                    console.error('Transaction failed abnormally', error);
                } else if (committed) {
                    claimed = true;
                }
            });

            if (!claimed) continue; // Skip if we didn't win the race

            // Now process safely
            try {
                let resultText = '';
                // Need to use the text from the original "queue" snapshot, 
                // but technically the snapshot in transaction is freshest. 
                // Let's assume text didn't change.

                if (item.action === 'encrypt') {
                    resultText = await encryptText(item.text, secret);
                } else if (item.action === 'decrypt') {
                    resultText = await decryptText(item.text, secret);
                }

                // Update result
                await itemRef.update({
                    status: 'completed',
                    result: resultText,
                    processedAt: Date.now(),
                    processedBy: task.name
                });

                processed++;
            } catch (err) {
                const errMsg = err.message || 'Unknown processing error';
                console.error(`⚠️ Processing error for item ${key}: ${errMsg}`);

                await itemRef.update({
                    status: 'failed',
                    error: errMsg,
                    processedAt: Date.now()
                });
                errors++;
            }
        }

        return {
            success: true,
            count: processed,
            message: `Processed ${processed} items (${errors} errors)`
        };

    } catch (e) {
        return { success: false, count: 0, message: `Encryption worker failed: ${e.message}` };
    }
}

// Auto-Drain Helper
async function drainEncryptionQueue(userId, tasks) {
    if (!tasks) return;
    const workerTask = Object.values(tasks).find(t => t.type === 'encryption_worker' && t.enabled);
    if (!workerTask) return;

    // console.log(`🚀 Starting Drain Queue for User: ${userId} via ${workerTask.name}`);

    // Loop until queue empty
    let draining = true;
    let totalProcessed = 0;

    // Safety break to prevent infinite loops (e.g. if 10k items, maybe let cron handle rest)
    let loops = 0;
    const MAX_LOOPS = 20; // 20 * 50 = 1000 items max burst

    while (draining && loops < MAX_LOOPS) {
        const result = await executeEncryptionTask(userId, workerTask);
        if (result.success && result.count > 0) {
            totalProcessed += result.count;
            loops++;
            // If we filled the batch (50), likely more exist.
            if (result.count < 50) draining = false;
        } else {
            draining = false;
        }
    }

    if (totalProcessed > 0) {
        // console.log(`✅ Drained ${totalProcessed} items for user ${userId}`);
    }
}

async function logExecution(userId, task, status, message, duration = 0) {
    await db.ref(`users/${userId}/logs`).push({
        taskId: task.id,
        taskName: task.name,
        type: task.type,
        status,
        message,
        duration, // Track execution time in ms
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
            // For encryption tasks, multiple tasks act as "workers" pulling from the same queue.
            // Conflict is prevented by the transactional 'claim' logic in executeEncryptionTask.
            await executeTask(userId, currentTask);
        }
    }, { scheduled: true, timezone: process.env.TIMEZONE || 'UTC' });

    activeJobs.set(jobKey, job);
    // console.log(`📅 Scheduled: ${task.name} (${task.schedule})`);
}

function cancelJob(jobKey) {
    const job = activeJobs.get(jobKey);
    if (job) {
        job.stop();
        activeJobs.delete(jobKey);
    }
}

// =========== WATCHERS & SCHEDULING SYSTEM ===========

const userTaskListeners = new Map(); // Track listeners to detach them if user is deleted

function initTaskSystem() {
    console.log('🚀 Initializing Task System...');

    // 1. Listen for new or existing users (child_added fires for existing data too)
    // We only access the key to avoid downloading the internal data if possible,
    // though Firebase SDK might still prefetch. But we DON'T map the whole object.
    db.ref('users').on('child_added', (snapshot) => {
        const userId = snapshot.key;
        setupUserTaskWatcher(userId);
    });

    // 2. Handle user deletion
    db.ref('users').on('child_removed', (snapshot) => {
        const userId = snapshot.key;
        cleanupUserTasks(userId);
    });
}

function setupUserTaskWatcher(userId) {
    if (userTaskListeners.has(userId)) return;

    // Listen ONLY to the 'tasks' node of the user.
    // This is CRITICAL: We avoid listening to 'users/{userId}' which contains 'logs'.
    // Logs update frequently and can be huge, causing OOM if we listen to the parent.
    const tasksRef = db.ref(`users/${userId}/tasks`);

    // We use 'value' to sync the tasks state. tasks list is generally small.
    const listener = tasksRef.on('value', (snapshot) => {
        const tasks = snapshot.val() || {};

        // reconcile jobs for this user
        const currentTaskIds = new Set();

        for (const [taskId, task] of Object.entries(tasks)) {
            currentTaskIds.add(taskId);

            // Only schedule if enabled
            if (task.enabled) {
                // Optimization: scheduleTask cancels existing job first, so this handles updates
                // Note: updating runCount/lastRun will trigger this. 
                // Ideally we should check if schedule/config changed, but scheduling is cheap enough here compared to OOM.
                scheduleTask(userId, task);
            } else {
                const jobKey = `${userId}_${task.id}`;
                cancelJob(jobKey);
            }
        }

        // Cleanup jobs for tasks that were deleted/removed from this snapshot
        // We verify against activeJobs for this user
        for (const [jobKey] of activeJobs) {
            if (jobKey.startsWith(`${userId}_`)) {
                const parts = jobKey.split('_');
                // userId is parts[0], taskId is parts[1] (or rest if id has underscore?)
                // Assuming simple ids. task.id usually pushId.
                const jobTaskId = parts[1];

                // If this job belongs to this user AND the task is no longer in the fetched tasks list
                if (parts[0] === userId && !currentTaskIds.has(jobTaskId)) {
                    cancelJob(jobKey);
                }
            }
        }
    });

    userTaskListeners.set(userId, { ref: tasksRef, listener });
    // console.log(`Listener attached for user: ${userId}`);
}

function cleanupUserTasks(userId) {
    // Detach Firebase listener
    if (userTaskListeners.has(userId)) {
        const { ref, listener } = userTaskListeners.get(userId);
        ref.off('value', listener);
        userTaskListeners.delete(userId);
    }

    // Stop all cron jobs for this user
    for (const [jobKey] of activeJobs) {
        if (jobKey.startsWith(`${userId}_`)) {
            cancelJob(jobKey);
        }
    }
    console.log(`Cleaned up user: ${userId}`);
}

// Serve frontend for all other routes
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
app.listen(PORT, async () => {
    console.log(`🚀 Joblix running on port ${PORT}`);
    console.log(`📍 http://localhost:${PORT}`);
    initTaskSystem();
});
