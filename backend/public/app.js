// Automo - Automated Task Scheduler
const FIREBASE_CONFIG = {
    apiKey: "AIzaSyBqD1J3iduH0QFAtE9utQBNc3ukRFwQz1M",
    authDomain: "cronify-4dcee.firebaseapp.com",
    databaseURL: "https://cronify-4dcee-default-rtdb.firebaseio.com",
    projectId: "cronify-4dcee",
    storageBucket: "cronify-4dcee.firebasestorage.app",
    messagingSenderId: "941814451722",
    appId: "1:941814451722:web:8a4acb3b4fc6275dd159f3",
    measurementId: "G-PPN8THV747"
};

// Pricing Config
const PRICING = {
    FREE_TASK_LIMIT: 5,           // Free users get 5 tasks
    CREDITS_PER_RUPEE: 100,       // 2000 credits for ₹20 = 100 per rupee
    MIN_PURCHASE: 20,             // Minimum ₹20
    CREDITS_PER_PURCHASE: 2000    // 2000 runs per ₹20
};

class Automo {
    constructor() {
        this.user = null;
        this.db = null;
        this.tasks = {};
        this.logs = [];
        this.userPlan = { plan: 'free', credits: 0 };
        this.init();
    }

    init() {
        // Initialize Firebase
        if (!firebase.apps.length) {
            firebase.initializeApp(FIREBASE_CONFIG);
        }
        this.db = firebase.database();

        // Auth state listener
        firebase.auth().onAuthStateChanged(user => {
            if (user) {
                this.user = user;
                this.showApp();
                this.loadUserData();
            } else {
                this.user = null;
                this.showAuth();
            }
        });

        this.bindEvents();
    }

    // =========== AUTH ===========
    bindAuthEvents() {
        // Tab switching
        document.querySelectorAll('.auth-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('.auth-tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.auth-form').forEach(f => f.classList.remove('active'));
                tab.classList.add('active');
                document.getElementById(`${tab.dataset.tab}-form`).classList.add('active');
                this.clearAuthError();
            });
        });

        // Login
        document.getElementById('login-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const email = document.getElementById('login-email').value;
            const password = document.getElementById('login-password').value;
            try {
                await firebase.auth().signInWithEmailAndPassword(email, password);
            } catch (error) {
                this.showAuthError(error.message);
            }
        });

        // Signup
        document.getElementById('signup-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const name = document.getElementById('signup-name').value;
            const email = document.getElementById('signup-email').value;
            const password = document.getElementById('signup-password').value;
            const confirmPassword = document.getElementById('signup-confirm-password').value;

            // Validate passwords match
            if (password !== confirmPassword) {
                this.showAuthError('Passwords do not match');
                return;
            }

            try {
                const cred = await firebase.auth().createUserWithEmailAndPassword(email, password);
                await cred.user.updateProfile({ displayName: name });
                // Initialize user data
                await this.db.ref(`users/${cred.user.uid}`).set({
                    name,
                    email,
                    createdAt: Date.now()
                });
            } catch (error) {
                this.showAuthError(error.message);
            }
        });

        // Google Sign-In
        document.getElementById('google-signin-btn').addEventListener('click', async () => {
            try {
                const provider = new firebase.auth.GoogleAuthProvider();
                const result = await firebase.auth().signInWithPopup(provider);
                // Check if new user, create profile
                const userRef = this.db.ref(`users/${result.user.uid}`);
                const snap = await userRef.once('value');
                if (!snap.exists()) {
                    await userRef.set({
                        name: result.user.displayName || result.user.email.split('@')[0],
                        email: result.user.email,
                        createdAt: Date.now()
                    });
                }
            } catch (error) {
                if (error.code !== 'auth/popup-closed-by-user') {
                    this.showAuthError(error.message);
                }
            }
        });
    }

    showAuthError(msg) {
        document.getElementById('auth-error').textContent = msg;
    }

    clearAuthError() {
        document.getElementById('auth-error').textContent = '';
    }

    showAuth() {
        document.getElementById('auth-container').classList.remove('hidden');
        document.getElementById('app').classList.add('hidden');
    }

    showApp() {
        document.getElementById('auth-container').classList.add('hidden');
        document.getElementById('app').classList.remove('hidden');
        this.updateUserInfo();
    }

    updateUserInfo() {
        const name = this.user.displayName || this.user.email.split('@')[0];
        document.getElementById('user-name').textContent = name;
        document.getElementById('user-avatar').textContent = name.charAt(0).toUpperCase();
    }

    // =========== DATA ===========
    async loadUserData() {
        // Listen to user plan
        this.db.ref(`users/${this.user.uid}/plan`).on('value', snap => {
            this.userPlan = snap.val() || { plan: 'free', credits: 0 };
            this.updatePlanDisplay();
        });

        // Listen to tasks
        this.db.ref(`users/${this.user.uid}/tasks`).on('value', snap => {
            this.tasks = snap.val() || {};
            this.renderTasks();
            this.updateStats();
        });

        // Listen to logs
        this.db.ref(`users/${this.user.uid}/logs`).orderByChild('timestamp').limitToLast(50).on('value', snap => {
            this.logs = [];
            snap.forEach(child => {
                this.logs.unshift({ id: child.key, ...child.val() });
            });
            this.renderLogs();
            this.renderRecentActivity();
        });
    }

    updatePlanDisplay() {
        const planBadge = document.getElementById('plan-badge');
        const creditsDisplay = document.getElementById('credits-display');
        if (planBadge) {
            planBadge.textContent = this.userPlan.plan === 'premium' ? '⭐ Premium' : '🆓 Free';
            planBadge.className = `plan-badge ${this.userPlan.plan}`;
        }
        if (creditsDisplay) {
            creditsDisplay.textContent = `${this.userPlan.credits || 0} credits`;
        }
    }

    getTaskCount() {
        return Object.values(this.tasks).filter(t => t && t.id).length;
    }

    canCreateTask() {
        const count = this.getTaskCount();
        if (count < PRICING.FREE_TASK_LIMIT) return true;
        if (this.userPlan.plan === 'premium' && this.userPlan.credits > 0) return true;
        return false;
    }

    updateStats() {
        const tasksArr = Object.values(this.tasks).filter(t => t && t.id);
        const freeSlots = Math.max(0, PRICING.FREE_TASK_LIMIT - tasksArr.length);

        document.getElementById('total-tasks').textContent = tasksArr.length;
        document.getElementById('active-tasks').textContent = tasksArr.filter(t => t.enabled).length;
        document.getElementById('url-tasks').textContent = tasksArr.filter(t => t.type === 'url').length;
        document.getElementById('firebase-tasks').textContent = tasksArr.filter(t => t.type === 'firebase').length;

        // Update free slots display if exists
        const freeSlotsEl = document.getElementById('free-slots');
        if (freeSlotsEl) freeSlotsEl.textContent = freeSlots;
    }

    async refreshTasks() {
        this.showToast('Refreshing...', 'success');
        const snap = await this.db.ref(`users/${this.user.uid}/tasks`).once('value');
        this.tasks = snap.val() || {};
        this.renderTasks();
        this.updateStats();
        this.showToast('Tasks refreshed!', 'success');
    }

    // =========== TASKS ===========
    async createTask(taskData) {
        // Check task limit
        if (!this.canCreateTask()) {
            this.showUpgradeModal();
            return;
        }

        const taskId = this.db.ref().push().key;
        const currentCount = this.getTaskCount();
        const isPaidTask = currentCount >= PRICING.FREE_TASK_LIMIT;

        const task = {
            id: taskId,
            ...taskData,
            userId: this.user.uid,
            createdAt: Date.now(),
            lastRun: null,
            runCount: 0,
            status: 'scheduled',
            isPaid: isPaidTask  // Mark if this task uses credits
        };
        await this.db.ref(`users/${this.user.uid}/tasks/${taskId}`).set(task);
        this.showToast('Task created!', 'success');
    }

    showUpgradeModal() {
        const count = this.getTaskCount();
        const modal = document.getElementById('upgrade-modal');
        if (modal) {
            document.getElementById('current-task-count').textContent = count;
            modal.classList.add('active');
        } else {
            this.showToast(`Task limit reached! You have ${count}/${PRICING.FREE_TASK_LIMIT} free tasks. Upgrade to add more.`, 'error');
        }
    }

    async buyCredits() {
        // For now, show a placeholder - integrate Razorpay here later
        const amount = PRICING.MIN_PURCHASE;
        const credits = PRICING.CREDITS_PER_PURCHASE;

        // Simulate payment success (replace with Razorpay integration)
        const confirmed = confirm(`Buy ${credits} credits for ₹${amount}?\n\nNote: This is a demo. In production, integrate Razorpay for actual payments.`);

        if (confirmed) {
            await this.addCredits(credits);
            document.getElementById('upgrade-modal').classList.remove('active');
            this.showToast(`🎉 Added ${credits} credits! You can now create more tasks.`, 'success');
        }
    }

    async addCredits(amount) {
        const currentCredits = this.userPlan.credits || 0;
        await this.db.ref(`users/${this.user.uid}/plan`).update({
            plan: 'premium',
            credits: currentCredits + amount,
            lastPurchase: Date.now()
        });
    }

    async updateTask(taskId, updates) {
        await this.db.ref(`users/${this.user.uid}/tasks/${taskId}`).update(updates);
        this.showToast('Task updated!', 'success');
    }

    async deleteTask(taskId) {
        if (!confirm('Delete this task?')) return;
        await this.db.ref(`users/${this.user.uid}/tasks/${taskId}`).remove();
        this.showToast('Task deleted!', 'success');
    }

    async toggleTask(taskId) {
        const task = this.tasks[taskId];
        if (!task) return;
        await this.updateTask(taskId, { enabled: !task.enabled });
    }

    async runTaskNow(taskId) {
        const task = this.tasks[taskId];
        if (!task) return;

        this.showToast('Running task...', 'success');

        try {
            const res = await fetch('/api/run-task', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ userId: this.user.uid, taskId })
            });
            const data = await res.json();
            if (data.success) {
                this.showToast('Task completed!', 'success');
            } else {
                this.showToast('Task failed: ' + data.error, 'error');
            }
        } catch (e) {
            this.showToast('Failed to run task: ' + e.message, 'error');
        }
    }

    // =========== RENDER ===========
    renderTasks() {
        const grid = document.getElementById('tasks-grid');
        // Filter out null/deleted tasks
        const tasksArr = Object.values(this.tasks).filter(task => task && task.id && task.name);

        if (tasksArr.length === 0) {
            grid.innerHTML = '<p class="empty">No tasks yet. Create one!</p>';
            return;
        }

        grid.innerHTML = tasksArr.map(task => {
            const typeLabel = task.type === 'url' ? 'URL' : 'Firebase';
            const typeIcon = task.type === 'url'
                ? '<i data-lucide="globe" class="icon"></i>'
                : '<i data-lucide="database" class="icon"></i>';
            const statusClass = task.enabled ? 'active' : 'paused';
            const statusText = task.enabled ? 'Active' : 'Paused';
            const metaInfo = task.type === 'url'
                ? (task.url || '').substring(0, 35) + '...'
                : (task.targetPath || task.action || '');
            const schedule = task.schedule || '';

            return `
      <div class="task-card ${task.enabled ? '' : 'disabled'}" data-id="${task.id}" data-type="${task.type}">
        <div class="task-header">
          <span class="task-name">${this.esc(task.name)}</span>
          <div>
            <span class="task-badge ${task.type}">${typeIcon} ${typeLabel}</span>
            <span class="task-badge ${statusClass}">${statusText}</span>
          </div>
        </div>
        <div class="task-meta">
          <span><i data-lucide="link" class="icon-sm"></i> ${this.esc(metaInfo)}</span>
          <span><i data-lucide="clock" class="icon-sm"></i> ${this.esc(schedule)}</span>
          <span><i data-lucide="repeat" class="icon-sm"></i> ${task.runCount || 0} runs</span>
        </div>
        <div class="task-actions">
          <button class="btn btn-sm btn-primary" onclick="app.runTaskNow('${task.id}')"><i data-lucide="play" class="icon-sm"></i> Run</button>
          <button class="btn btn-sm btn-outline" onclick="app.toggleTask('${task.id}')">${task.enabled ? '<i data-lucide="pause" class="icon-sm"></i>' : '<i data-lucide="play" class="icon-sm"></i>'}</button>
          <button class="btn btn-sm btn-outline" onclick="app.editTask('${task.id}')"><i data-lucide="edit-2" class="icon-sm"></i></button>
          <button class="btn btn-sm btn-outline" onclick="app.deleteTask('${task.id}')"><i data-lucide="trash-2" class="icon-sm"></i></button>
        </div>
      </div>
    `;
        }).join('');

        // Initialize Lucide icons
        if (typeof lucide !== 'undefined') lucide.createIcons();
    }

    renderLogs() {
        const tbody = document.getElementById('logs-tbody');
        if (this.logs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="empty">No logs yet</td></tr>';
            return;
        }

        tbody.innerHTML = this.logs.map(log => `
      <tr>
        <td><span class="log-status ${log.status}">${log.status}</span></td>
        <td>${this.esc(log.taskName)}</td>
        <td>${log.type === 'url' ? '🌐' : '🔥'} ${log.type}</td>
        <td>${this.esc(log.message || '').substring(0, 40)}</td>
        <td>${this.formatTime(log.timestamp)}</td>
      </tr>
    `).join('');
    }

    renderRecentActivity() {
        const container = document.getElementById('recent-activity');
        const recent = this.logs.slice(0, 5);

        if (recent.length === 0) {
            container.innerHTML = '<p class="empty">No recent activity</p>';
            return;
        }

        container.innerHTML = recent.map(log => `
      <div class="activity-item">
        <div class="activity-icon">${log.status === 'success' ? '✅' : '❌'}</div>
        <div class="activity-info">
          <div class="activity-name">${this.esc(log.taskName)}</div>
          <div class="activity-time">${this.formatTime(log.timestamp)}</div>
        </div>
      </div>
    `).join('');
    }

    // =========== UI ===========
    bindEvents() {
        this.bindAuthEvents();

        // Navigation
        document.querySelectorAll('.nav-item').forEach(item => {
            item.addEventListener('click', (e) => {
                e.preventDefault();
                this.switchSection(item.dataset.section);
            });
        });

        // Mobile menu
        document.getElementById('menu-btn').addEventListener('click', () => {
            document.getElementById('sidebar').classList.toggle('open');
        });

        // Logout
        document.getElementById('logout-btn').addEventListener('click', () => {
            firebase.auth().signOut();
        });

        // New task
        document.getElementById('new-task-btn').addEventListener('click', () => this.openTaskModal());

        // Refresh tasks
        document.getElementById('refresh-tasks-btn')?.addEventListener('click', () => this.refreshTasks());

        // Task modal
        document.getElementById('modal-close').addEventListener('click', () => this.closeTaskModal());
        document.getElementById('modal-cancel').addEventListener('click', () => this.closeTaskModal());
        document.getElementById('modal-save').addEventListener('click', () => this.saveTask());

        // Task type selection
        document.querySelectorAll('.type-option').forEach(opt => {
            opt.addEventListener('click', () => {
                document.querySelectorAll('.type-option').forEach(o => o.classList.remove('active'));
                opt.classList.add('active');
                document.getElementById('task-type').value = opt.dataset.type;
                this.toggleTaskFields(opt.dataset.type);
            });
        });

        // Action change
        document.getElementById('task-action')?.addEventListener('change', (e) => {
            document.getElementById('days-field').style.display = e.target.value === 'delete_old' ? 'block' : 'none';
        });

        // Delete account
        document.getElementById('delete-account-btn')?.addEventListener('click', () => this.deleteAccount());

        // Clear logs
        document.getElementById('clear-logs-btn')?.addEventListener('click', () => this.clearLogs());

        // Filters
        document.querySelectorAll('.filter-btn').forEach(btn => {
            btn.addEventListener('click', () => this.filterTasks(btn.dataset.filter));
        });

        // Close modals on overlay click
        document.querySelectorAll('.modal-overlay').forEach(overlay => {
            overlay.addEventListener('click', (e) => {
                if (e.target === overlay) overlay.classList.remove('active');
            });
        });
    }

    switchSection(section) {
        document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
        document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
        document.getElementById(`${section}-section`).classList.add('active');
        document.querySelector(`[data-section="${section}"]`).classList.add('active');
        document.getElementById('page-title').textContent = section.charAt(0).toUpperCase() + section.slice(1);
        document.getElementById('sidebar').classList.remove('open');
    }

    toggleTaskFields(type) {
        document.getElementById('url-fields').style.display = type === 'url' ? 'block' : 'none';
        document.getElementById('firebase-fields').style.display = type === 'firebase' ? 'block' : 'none';
    }

    openTaskModal(task = null) {
        const modal = document.getElementById('task-modal');
        document.getElementById('modal-title').textContent = task ? 'Edit Task' : 'New Task';
        document.getElementById('modal-save').textContent = task ? 'Update' : 'Create';

        if (task) {
            document.getElementById('task-id').value = task.id;
            document.getElementById('task-name').value = task.name;
            document.getElementById('task-type').value = task.type;
            document.getElementById('task-schedule').value = task.schedule;
            document.getElementById('task-enabled').checked = task.enabled;

            // Set type option
            document.querySelectorAll('.type-option').forEach(o => {
                o.classList.toggle('active', o.dataset.type === task.type);
            });
            this.toggleTaskFields(task.type);

            if (task.type === 'url') {
                document.getElementById('task-url').value = task.url || '';
            } else {
                document.getElementById('task-firebase-config').value = task.firebaseConfig || '';
                document.getElementById('task-service-account').value = task.serviceAccount || '';
                document.getElementById('task-action').value = task.action || 'delete';
                document.getElementById('task-target-path').value = task.targetPath || '';
                document.getElementById('task-days').value = task.olderThanDays || 30;
            }
        } else {
            document.getElementById('task-form').reset();
            document.getElementById('task-id').value = '';
            document.querySelectorAll('.type-option').forEach((o, i) => o.classList.toggle('active', i === 0));
            this.toggleTaskFields('url');
        }

        modal.classList.add('active');
    }

    closeTaskModal() {
        document.getElementById('task-modal').classList.remove('active');
    }

    async saveTask() {
        const taskId = document.getElementById('task-id').value;
        const type = document.getElementById('task-type').value;

        const taskData = {
            name: document.getElementById('task-name').value,
            type,
            schedule: document.getElementById('task-schedule').value,
            enabled: document.getElementById('task-enabled').checked
        };

        if (type === 'url') {
            taskData.url = document.getElementById('task-url').value;
            if (!taskData.url) {
                this.showToast('Please enter a URL', 'error');
                return;
            }
        } else {
            taskData.firebaseConfig = document.getElementById('task-firebase-config').value;
            taskData.serviceAccount = document.getElementById('task-service-account').value;
            taskData.action = document.getElementById('task-action').value;
            taskData.targetPath = document.getElementById('task-target-path').value;
            if (taskData.action === 'delete_old') {
                taskData.olderThanDays = parseInt(document.getElementById('task-days').value) || 30;
            }
            if (!taskData.firebaseConfig || !taskData.serviceAccount || !taskData.targetPath) {
                this.showToast('Please fill all Firebase fields', 'error');
                return;
            }
        }

        if (!taskData.name || !taskData.schedule) {
            this.showToast('Please fill name and schedule', 'error');
            return;
        }

        try {
            if (taskId) {
                await this.updateTask(taskId, taskData);
            } else {
                await this.createTask(taskData);
            }
            this.closeTaskModal();
        } catch (e) {
            this.showToast('Error: ' + e.message, 'error');
        }
    }

    editTask(taskId) {
        const task = this.tasks[taskId];
        if (task) this.openTaskModal(task);
    }

    filterTasks(filter) {
        document.querySelectorAll('.filter-btn').forEach(b => b.classList.toggle('active', b.dataset.filter === filter));
        document.querySelectorAll('.task-card').forEach(card => {
            const type = card.dataset.type;
            if (filter === 'all') card.style.display = '';
            else card.style.display = type === filter ? '' : 'none';
        });
    }

    async clearLogs() {
        if (!confirm('Clear all logs?')) return;
        await this.db.ref(`users/${this.user.uid}/logs`).remove();
        this.showToast('Logs cleared!', 'success');
    }

    async deleteAccount() {
        if (!confirm('Delete your account and all data? This cannot be undone!')) return;
        try {
            await this.db.ref(`users/${this.user.uid}`).remove();
            await this.user.delete();
            this.showToast('Account deleted', 'success');
        } catch (e) {
            this.showToast('Error: ' + e.message, 'error');
        }
    }

    // =========== UTILS ===========
    showToast(message, type = 'success') {
        const container = document.getElementById('toasts');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;
        container.appendChild(toast);
        setTimeout(() => toast.remove(), 4000);
    }

    formatTime(timestamp) {
        if (!timestamp) return 'Never';
        return new Date(timestamp).toLocaleString();
    }

    esc(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }
}

// Initialize
const app = new Automo();
