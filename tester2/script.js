import { initializeApp } from "https://www.gstatic.com/firebasejs/9.22.0/firebase-app.js";
import { getDatabase, ref, set, get, update, onValue, remove, off, runTransaction, push, onDisconnect, query, orderByChild, startAt, endAt, limitToLast } from "https://www.gstatic.com/firebasejs/9.22.0/firebase-database.js";
import { getAuth, createUserWithEmailAndPassword, signInWithEmailAndPassword, signOut, onAuthStateChanged } from "https://www.gstatic.com/firebasejs/9.22.0/firebase-auth.js";

const firebaseConfig = {
    apiKey: "AIzaSyDZF8tuhDKrcDOwkVVsSoZGHtdbAFOzHZ8", 
    authDomain: "arz30maping.firebaseapp.com",
    databaseURL: "https://arz30maping-default-rtdb.europe-west1.firebasedatabase.app",
    projectId: "arz30maping",
    storageBucket: "arz30maping.firebasestorage.app",
    messagingSenderId: "633313086264",
    appId: "1:633313086264:web:5c3b657d38403a0f2c4fcd",
    measurementId: "G-VYXSB2BDRJ"
};

const app = initializeApp(firebaseConfig);
const db = getDatabase(app);
const auth = getAuth(app);
const DEFAULT_AVATAR = "https://cdn-icons-png.flaticon.com/512/847/847969.png";

let currentUser = {};
let hasAccess = false; 
let isMaintenanceActive = false;
let userListenerRef = null;
let workerListenerRef = null;
let recordsListenerRef = null;
let workersListRef = null;
let archiveListenerRef = null; 
let currentSiteVersion = null;
let usersDataCache = {}; 
let workersDataCache = {}; 
let cropper = null; 
let currentCategory = '';
let uploadCategory = ''; 
let activeInput = null;
let currentArchiveData = null;
let currentArchiveTab = 'houses';
let currentPage = 1;
// ИЗМЕНЕНО: теперь берем из настроек или 100
let itemsPerPage = parseInt(localStorage.getItem('rowsPerPage')) || 100;
let allRecords = [];      
let currentRecords = [];  
let isDragging = false;
let dragValue = null; 
let dragSelection = []; 
let isEraserMode = false;
let statusListenerRef = null; 

// === НАСТРОЙКИ (SETTINGS) ===
// Загрузка темы при старте
const savedTheme = localStorage.getItem('siteTheme') || 'default';
document.body.className = savedTheme !== 'default' ? `theme-${savedTheme}` : '';

const createEmail = (nickname) => `${nickname.toLowerCase()}@arzproject.com`;

function translateCategory(cat) {
    if (cat === 'houses') return 'Дом';
    if (cat === 'biz') return 'Бизнес';
    if (cat === 'trailers') return 'Трейлер';
    return cat;
}

function escapeHtml(text) {
    if (!text) return text;
    return text.toString().replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}

function validateVkId(val) {
    if (!val) return "VK ID обязателен";
    const strVal = val.toString().trim();
    if (!/^\d+$/.test(strVal)) return "VK ID должен содержать только цифры!";
    if (strVal.length < 4) return "VK ID слишком короткий (минимум 4 цифры)";
    const banned = ['1234', '12345', '123123', '0000', '1111', '2222', '3333', '4444', '5555'];
    if (banned.includes(strVal)) return "Укажите точный ваш VK ID (не 1234)";
    return null; 
}

function isRecordLocked(rec) {
    if (!rec || !rec.checkedBy) return false;
    if (currentUser.role === 'admin') return false;
    if (rec.checkedBy === currentUser.nickname) return false;
    const myWorkerProfile = workersDataCache[currentUser.nickname];
    if (myWorkerProfile) {
        const pos = myWorkerProfile.position;
        if (['ЗГС Маппинга', 'ГС Маппинга', 'Куратор'].includes(pos)) {
            return false;
        }
    }
    return true;
}

// === ФУНКЦИИ НАСТРОЕК ===
window.openSettingsModal = () => {
    document.getElementById('profile-modal').classList.add('hidden');
    document.getElementById('settings-modal').classList.remove('hidden');
    // Установить активный класс на текущую тему
    const currentTheme = localStorage.getItem('siteTheme') || 'default';
    document.querySelectorAll('.theme-btn').forEach(btn => btn.classList.remove('active-theme'));
    // Находим кнопку по цвету или атрибуту (для простоты перебираем title)
    const btns = document.querySelectorAll('.theme-btn');
    if(currentTheme === 'default') btns[0].classList.add('active-theme');
    if(currentTheme === 'pink') btns[1].classList.add('active-theme');
    if(currentTheme === 'green') btns[2].classList.add('active-theme');
    if(currentTheme === 'red') btns[3].classList.add('active-theme');
    if(currentTheme === 'black') btns[4].classList.add('active-theme');
    
    // Установить select
    document.getElementById('rows-per-page-select').value = itemsPerPage;
}

window.closeSettingsModal = () => {
    document.getElementById('settings-modal').classList.add('hidden');
    document.getElementById('profile-modal').classList.remove('hidden');
}

window.applyTheme = (themeName) => {
    localStorage.setItem('siteTheme', themeName);
    document.body.className = themeName !== 'default' ? `theme-${themeName}` : '';
    
    // Обновляем галочки визуально
    document.querySelectorAll('.theme-btn').forEach(btn => btn.classList.remove('active-theme'));
    const btns = document.querySelectorAll('.theme-btn');
    if(themeName === 'default') btns[0].classList.add('active-theme');
    if(themeName === 'pink') btns[1].classList.add('active-theme');
    if(themeName === 'green') btns[2].classList.add('active-theme');
    if(themeName === 'red') btns[3].classList.add('active-theme');
    if(themeName === 'black') btns[4].classList.add('active-theme');
}

window.changeRowsPerPage = (val) => {
    itemsPerPage = parseInt(val);
    localStorage.setItem('rowsPerPage', itemsPerPage);
    if (currentCategory && currentCategory !== 'archive') {
        renderCurrentPage(); // Перерисовать таблицу сразу
    }
}

// === СТАНДАРТНЫЕ ФУНКЦИИ ===
window.showRegister = () => { document.getElementById('login-form').classList.add('hidden'); document.getElementById('register-form').classList.remove('hidden'); }
window.showLogin = () => { document.getElementById('register-form').classList.add('hidden'); document.getElementById('login-form').classList.remove('hidden'); }

window.registerUser = async () => {
    const nick = document.getElementById('reg-nick').value.trim();
    const pass = document.getElementById('reg-pass').value.trim();
    const vk = document.getElementById('reg-vk').value.trim();
    
    if (!nick || !pass) return alert("Заполните все поля!");
    const vkError = validateVkId(vk);
    if (vkError) return alert(vkError);

    try {
        const userCredential = await createUserWithEmailAndPassword(auth, createEmail(nick), pass);
        await createUserInDB(userCredential.user.uid, nick, vk);
        alert("Регистрация успешна!");
    } catch (error) {
        if (error.code === 'auth/email-already-in-use') {
            try {
                const loginCred = await signInWithEmailAndPassword(auth, createEmail(nick), pass);
                const uid = loginCred.user.uid;
                const snapshot = await get(ref(db, 'users/' + uid));
                if (!snapshot.exists()) { await createUserInDB(uid, nick, vk); alert("Аккаунт восстановлен!"); }
                else { alert("Аккаунт существует. Вы вошли."); }
            } catch (e) { alert("Ник занят."); }
        } else { alert(error.message); }
    }
}
async function createUserInDB(uid, nick, vk) { await set(ref(db, 'users/' + uid), { nickname: nick, vkId: vk, role: "user", avatar: DEFAULT_AVATAR }); }

window.loginUser = async () => {
    const nick = document.getElementById('login-nick').value.trim();
    const pass = document.getElementById('login-pass').value.trim();
    try { await signInWithEmailAndPassword(auth, createEmail(nick), pass); } 
    catch (error) { alert("Неверный логин или пароль"); }
}
window.logout = () => { closeProfile(); signOut(auth); }

function setupPresence(uid, nickname) {
    const connectedRef = ref(db, ".info/connected");
    const userStatusRef = ref(db, "status/" + nickname);

    onValue(connectedRef, (snap) => {
        if (snap.val() === true) {
            const con = onDisconnect(userStatusRef);
            con.set({ state: 'offline', last_changed: Date.now() });
            set(userStatusRef, { state: 'online', last_changed: Date.now() });
        }
    });
}

function updateInterfaceAccess() {
    if (!currentUser || !currentUser.nickname) return;
    let workerData = workersDataCache[currentUser.nickname] || {};
    let position = workerData.position || "";
    let isAdmin = currentUser.role === 'admin';
    let isManagement = isAdmin || ['ЗГС Маппинга', 'ГС Маппинга', 'Куратор'].includes(position);

    const btnUpload = document.getElementById('btn-upload-access');
    if (btnUpload) {
        if (isManagement) btnUpload.classList.remove('hidden');
        else btnUpload.classList.add('hidden');
    }
    const btnAdminPanel = document.getElementById('btn-admin-panel');
    if (btnAdminPanel) {
        if (isAdmin) btnAdminPanel.classList.remove('hidden');
        else btnAdminPanel.classList.add('hidden');
    }
    const btnAddWorker = document.getElementById('btn-to-add-worker');
    if (btnAddWorker) {
        if (isManagement) btnAddWorker.classList.remove('hidden');
        else btnAddWorker.classList.add('hidden');
    }
}

window.trySwitchTab = (tabName) => {
    if (isMaintenanceActive && currentUser.role !== 'admin') return;
    if (!hasAccess && tabName !== 'access') { document.getElementById('access-denied-popup').classList.remove('hidden'); return; }
    switchTab(tabName);
}

window.switchTab = (tabName) => {
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    const content = document.getElementById('content-area');
    const lockScreen = document.getElementById('lock-screen');
    const tabAccess = document.getElementById('tab-access');
    const dateContainer = document.getElementById('date-display-container');
    const searchControls = document.getElementById('search-controls');
    
    if (recordsListenerRef) { off(recordsListenerRef); recordsListenerRef = null; }
    if (archiveListenerRef && tabName !== 'archive') { off(archiveListenerRef); archiveListenerRef = null; }

    if(dateContainer) dateContainer.classList.add('hidden');
    if(searchControls) searchControls.classList.add('hidden');

    if (tabName === 'access') {
        lockScreen.classList.remove('hidden'); content.classList.add('hidden');
        if(tabAccess) tabAccess.classList.add('active');
        return;
    }

    lockScreen.classList.add('hidden'); 
    content.classList.remove('hidden');
    const btn = document.getElementById('tab-' + tabName);
    if(btn) btn.classList.add('active');

    const main = document.querySelector('main');
    if(main) main.scrollTo({ top: 0 });

    if (['houses', 'biz', 'trailers'].includes(tabName)) {
        currentCategory = tabName;
        currentPage = 1; 
        if(dateContainer) dateContainer.classList.remove('hidden');
        if(searchControls) searchControls.classList.remove('hidden');
        
        document.getElementById('table-search-input').value = '';
        document.getElementById('filter-unchecked').checked = false;
        document.getElementById('filter-checked-only').checked = false;
        
        content.innerHTML = `<div id="table-container-${tabName}"><div class="loader" style="margin-top:50px;"></div></div><div id="pagination-container" class="pagination-controls hidden"></div>`;
        startTableListener(tabName);
    } else if (tabName === 'archive') {
        currentCategory = 'archive';
        renderArchivePage();
    }
}

async function logAction(action, details) {
    if (!currentUser.nickname) return;
    const logEntry = { user: currentUser.nickname, action: action, desc: details, time: Date.now() };
    try { 
        await push(ref(db, 'logs'), logEntry); 
        console.log("LOG SAVED:", details);
    } catch(e) { 
        console.error("LOG FAILED:", e); 
    }
}

// === УВЕДОМЛЕНИЯ (NOTIFICATIONS) - УМНЫЕ ===
function startNotificationListener() {
    const notifRef = query(ref(db, 'notifications'), orderByChild('time'), limitToLast(50));
    
    onValue(notifRef, (snap) => {
        let unreadCount = 0;
        const lastReadTime = parseInt(localStorage.getItem('notifLastReadTime')) || 0;
        const allNotifs = [];

        snap.forEach(c => {
            const val = c.val();
            allNotifs.push({ key: c.key, ...val });
            if (val.time > lastReadTime) {
                unreadCount++;
            }
        });

        // Сортировка: новые сверху
        allNotifs.sort((a,b) => b.time - a.time);

        // Обновление интерфейса колокольчика
        updateBellUI(unreadCount);

        // Если окно открыто - сразу рисуем список
        if (!document.getElementById('notifications-modal').classList.contains('hidden')) {
            renderNotificationsToList(allNotifs);
        }
    });
}

function updateBellUI(count) {
    const badge = document.getElementById('notif-badge');
    const btn = document.getElementById('notif-btn-main');
    
    if (count > 0) {
        badge.innerText = count > 9 ? '9+' : count;
        badge.classList.remove('hidden');
        btn.classList.add('ringing'); // Включаем анимацию
    } else {
        badge.classList.add('hidden');
        btn.classList.remove('ringing'); // Выключаем анимацию
    }
}

window.toggleNotifications = () => {
    const modal = document.getElementById('notifications-modal');
    if (modal.classList.contains('hidden')) {
        // ОТКРЫВАЕМ
        modal.classList.remove('hidden');
        
        // Сразу загружаем и показываем
        const list = document.getElementById('notifications-list-container');
        list.innerHTML = '<div class="loader"></div>';
        
        get(query(ref(db, 'notifications'), orderByChild('time'), limitToLast(50))).then(snap => {
            const notifs = [];
            snap.forEach(c => notifs.push({ key: c.key, ...c.val() }));
            notifs.sort((a,b) => b.time - a.time);
            renderNotificationsToList(notifs);
            
            // Фиксируем время прочтения (текущее время)
            const now = Date.now();
            localStorage.setItem('notifLastReadTime', now);
            updateBellUI(0); // Сбрасываем счетчик
        });

    } else {
        // ЗАКРЫВАЕМ
        modal.classList.add('hidden');
    }
}

function renderNotificationsToList(notifs) {
    const list = document.getElementById('notifications-list-container');
    list.innerHTML = '';
    
    if (notifs.length === 0) {
        list.innerHTML = '<p style="text-align:center; color:#888;">Уведомлений нет</p>';
        return;
    }

    notifs.forEach(n => {
        const date = new Date(n.time).toLocaleDateString('ru-RU') + ' ' + new Date(n.time).toLocaleTimeString('ru-RU', {hour: '2-digit', minute:'2-digit'});
        const card = document.createElement('div');
        card.className = 'notif-card';
        let delBtn = '';
        if(currentUser.role === 'admin') {
            delBtn = `<button class="mini-btn del-mini" onclick="deleteNotification('${n.key}')" style="float:right;"><i class="fa-solid fa-trash"></i></button>`;
        }

        card.innerHTML = `
            <div class="notif-header">
                <span class="notif-title">${escapeHtml(n.title)}</span>
                <span class="notif-date">${date}</span>
            </div>
            ${delBtn}
            <div class="notif-body">${escapeHtml(n.text)}</div>
            <div class="notif-author">От: ${escapeHtml(n.author)}</div>
        `;
        list.appendChild(card);
    });
}

window.openSendNotificationModal = () => {
    document.getElementById('admin-modal').classList.add('hidden');
    document.getElementById('send-notif-modal').classList.remove('hidden');
}

window.closeSendNotificationModal = () => {
    document.getElementById('send-notif-modal').classList.add('hidden');
    document.getElementById('admin-modal').classList.remove('hidden');
}

window.sendNotification = async () => {
    const title = document.getElementById('notif-title-input').value.trim();
    const text = document.getElementById('notif-text-input').value.trim();
    
    if(!title || !text) return alert("Заполните заголовок и текст!");

    const newNotif = {
        title: title,
        text: text,
        author: currentUser.nickname,
        time: Date.now()
    };

    try {
        await push(ref(db, 'notifications'), newNotif);
        alert("Уведомление отправлено!");
        document.getElementById('notif-title-input').value = '';
        document.getElementById('notif-text-input').value = '';
        closeSendNotificationModal();
    } catch(e) {
        alert("Ошибка: " + e.message);
    }
}

window.deleteNotification = (key) => {
    if(!confirm("Удалить это уведомление?")) return;
    remove(ref(db, 'notifications/' + key));
}


// === ОСТАЛЬНЫЕ ФУНКЦИИ (ЛОГИ, АРХИВЫ и т.д.) ===

window.openLogsModal = () => { 
    document.getElementById('admin-modal').classList.add('hidden'); 
    document.getElementById('logs-modal').classList.remove('hidden'); 
    document.getElementById('logs-list').innerHTML = '<p style="text-align:center; color:#888; margin-top:50px;">Нажмите "Загрузить все" или используйте поиск</p>';
}
window.closeLogsModal = () => { document.getElementById('logs-modal').classList.add('hidden'); document.getElementById('admin-modal').classList.remove('hidden'); }
window.clearLogs = async () => { if(!confirm("Вы уверены?")) return; alert("Из соображений безопасности очистка логов отключена в правилах базы данных."); }

window.loadAllLogs = () => {
    const list = document.getElementById('logs-list');
    list.innerHTML = '<div class="loader"></div>';
    const q = query(ref(db, 'logs'), orderByChild('time'), limitToLast(200));
    get(q).then(snapshot => {
        renderLogsSnapshot(snapshot);
    }).catch(e => {
        list.innerHTML = `<p style="text-align:center; color:red; margin-top:20px;">Ошибка: ${e.message}<br>Проверьте Rules (нужен indexOn: ["time"])</p>`;
    });
}

window.searchLogs = () => {
    const nick = document.getElementById('log-search-nick').value.trim().toLowerCase();
    const dateFrom = document.getElementById('log-date-from').value;
    const dateTo = document.getElementById('log-date-to').value;
    const list = document.getElementById('logs-list');

    if (!dateFrom && !dateTo && !nick) { return loadAllLogs(); }

    list.innerHTML = '<div class="loader"></div>';

    let q = ref(db, 'logs');
    if (dateFrom || dateTo) {
        let start = dateFrom ? new Date(dateFrom).setHours(0,0,0,0) : 0;
        let end = dateTo ? new Date(dateTo).setHours(23,59,59,999) : Date.now();
        q = query(ref(db, 'logs'), orderByChild('time'), startAt(start), endAt(end));
    } else {
        q = query(ref(db, 'logs'), orderByChild('time'), limitToLast(300));
    }

    get(q).then(snapshot => {
        if(!snapshot.exists()) { list.innerHTML = '<p style="text-align:center; margin-top:20px;">Ничего не найдено</p>'; return; }
        let logs = [];
        snapshot.forEach(c => logs.push(c.val()));
        if (nick) {
            logs = logs.filter(l => 
                (l.user && l.user.toLowerCase().includes(nick)) || 
                (l.desc && l.desc.toLowerCase().includes(nick))
            );
        }
        logs.sort((a,b) => b.time - a.time);
        renderLogsToList(logs);
    }).catch(e => {
        list.innerHTML = `<p style="text-align:center; color:red;">Ошибка: ${e.message}</p>`;
    });
}

function renderLogsSnapshot(snapshot) {
    const list = document.getElementById('logs-list');
    if(!snapshot.exists()) { list.innerHTML = '<p style="text-align:center; color:#888; margin-top:20px;">Логов нет</p>'; return; }
    const logs = [];
    snapshot.forEach(c => logs.push(c.val()));
    logs.sort((a,b) => b.time - a.time);
    renderLogsToList(logs);
}

function renderLogsToList(logs) {
    const list = document.getElementById('logs-list');
    list.innerHTML = '';
    if(logs.length === 0) { list.innerHTML = '<p style="text-align:center; margin-top:20px;">Ничего не найдено</p>'; return; }
    
    logs.forEach(log => {
        const date = new Date(log.time).toLocaleString('ru-RU');
        const item = document.createElement('div');
        item.className = 'log-item';
        let badgeClass = 'log-badge';
        if(log.action === 'DELETE') badgeClass += ' badge-del';
        else if(log.action === 'CLAIM') badgeClass += ' badge-claim';
        else if(log.action === 'EDIT') badgeClass += ' badge-edit';
        else if(log.action === 'UPLOAD') badgeClass += ' badge-upload';
        else if(log.action === 'STATUS') badgeClass += ' badge-status';
        else if(log.action === 'DELETE_ATTEMPT') badgeClass += ' badge-del';
        
        const safeUser = escapeHtml(log.user);
        const safeDesc = escapeHtml(log.desc);
        
        item.innerHTML = `<div class="log-content"><div class="log-header"><span class="${badgeClass}">${log.action}</span><span>${safeUser}</span></div><div class="log-desc">${safeDesc}</div></div><div class="log-time">${date}</div>`;
        list.appendChild(item);
    });
}

function renderArchivePage() {
    const content = document.getElementById('content-area');
    let workerData = workersDataCache[currentUser.nickname] || {};
    let position = workerData.position || "";
    let isManagement = currentUser.role === 'admin' || ['ЗГС Маппинга', 'ГС Маппинга', 'Куратор'].includes(position);
    
    let html = `<h1>📁 Архив</h1>`;
    if(isManagement) html += `<button class="create-archive-btn" onclick="createArchive()"><i class="fa-solid fa-box-archive"></i> Создать архив сезона</button>`;
    html += `<div id="archive-list" class="archive-list"><div class="loader"></div></div>`;
    content.innerHTML = html;
    if (archiveListenerRef) off(archiveListenerRef);
    archiveListenerRef = ref(db, 'archives');
    onValue(archiveListenerRef, (snapshot) => {
        const list = document.getElementById('archive-list');
        if(!list) return;
        list.innerHTML = '';
        if(!snapshot.exists()) { list.innerHTML = '<p style="color:#aaa; width:100%;">Архивов пока нет</p>'; return; }
        const archives = [];
        snapshot.forEach(child => { archives.push({ key: child.key, ...child.val() }); });
        archives.sort((a, b) => b.timestamp - a.timestamp);
        archives.forEach(arch => {
            const date = new Date(arch.timestamp).toLocaleDateString('ru-RU');
            const card = document.createElement('div');
            card.className = 'archive-card';
            let delBtn = '';
            if (currentUser.role === 'admin') delBtn = `<button class="archive-delete-btn" title="Удалить архив" onclick="deleteArchive(event, '${arch.key}')"><i class="fa-solid fa-trash"></i></button>`;
            card.innerHTML = `${delBtn}<div class="archive-title">${escapeHtml(arch.name) || 'Без названия'}</div><div class="archive-date">Создан: ${date}</div>`;
            card.onclick = () => viewArchive(arch);
            list.appendChild(card);
        });
    });
}

window.deleteArchive = (e, key) => { e.stopPropagation(); if(!confirm("Удалить этот архив навсегда?")) return; remove(ref(db, 'archives/' + key)).catch(err => alert("Ошибка: " + err.message)); }
window.createArchive = async () => {
    const dateElement = document.getElementById('header-date-text');
    const dateRange = dateElement ? dateElement.innerText : "Неизвестная дата";
    const archiveName = `Архив ${dateRange}`;
    if(!confirm(`ВНИМАНИЕ! Это действие сохранит копию текущей таблицы в "${archiveName}".`)) return;
    try {
        const housesSnap = await get(ref(db, 'records/houses'));
        const bizSnap = await get(ref(db, 'records/biz'));
        const trailersSnap = await get(ref(db, 'records/trailers'));
        const archiveData = { name: archiveName, timestamp: Date.now(), data: { houses: housesSnap.val() || {}, biz: bizSnap.val() || {}, trailers: trailersSnap.val() || {} } };
        await set(ref(db, 'archives/' + Date.now()), archiveData);
        alert("Архив успешно создан!");
    } catch (e) { alert("Ошибка при создании архива: " + e.message); }
}

window.viewArchive = (arch) => { currentArchiveData = arch.data; currentArchiveTab = 'houses'; document.getElementById('archive-view-title').innerText = arch.name; document.getElementById('archive-view-modal').classList.remove('hidden'); switchArchiveTab('houses'); }
window.closeArchiveViewer = () => { document.getElementById('archive-view-modal').classList.add('hidden'); currentArchiveData = null; }
window.switchArchiveTab = (tab) => {
    currentArchiveTab = tab;
    const tabs = document.querySelectorAll('#archive-tabs .tab-btn');
    tabs.forEach(t => t.classList.remove('active'));
    if(tab === 'houses') tabs[0].classList.add('active');
    if(tab === 'biz') tabs[1].classList.add('active');
    if(tab === 'trailers') tabs[2].classList.add('active');
    renderArchiveTable();
}

window.renderArchiveTable = () => {
    const container = document.getElementById('archive-content-area');
    container.innerHTML = '<div class="loader" style="margin-top:50px;"></div>';
    const isAdmin = currentUser.role === 'admin';
    if (!currentArchiveData || !currentArchiveData[currentArchiveTab]) { container.innerHTML = '<div class="empty-state">В этом разделе архива пусто</div>'; return; }
    const dataObj = currentArchiveData[currentArchiveTab];
    const records = Object.values(dataObj).sort((a, b) => (parseInt(a.gameId) || 0) - (parseInt(b.gameId) || 0));
    let statusHeader = isAdmin ? '<th style="text-align:center;">Статус</th>' : '';
    let html = `<div class="table-responsive"><table class="custom-table" style="min-width: 800px;"><thead><tr><th style="width: 50px;">ID</th><th>Владелец</th><th>Название</th><th>Тип нарушения</th><th>Доказательства</th><th>Кто проверил?</th><th>Ответственный</th>${statusHeader}</tr></thead><tbody>`;
    records.forEach(rec => {
        let proofDisplay = '-';
        if (rec.proof && rec.proof.startsWith('http')) proofDisplay = `<a href="${escapeHtml(rec.proof)}" target="_blank" class="proof-link"><i class="fa-solid fa-link"></i> Ссылка</a>`; else proofDisplay = escapeHtml(rec.proof) || '-';
        let violClass = 'viol-gray';
        if (rec.violation === 'Маппинг') violClass = 'viol-blue'; else if (rec.violation === 'Названия') violClass = 'viol-red'; else if (rec.violation === 'Маппинг + названия') violClass = 'viol-gradient'; 
        const violDisplay = `<span class="${violClass}">${rec.violation || 'Нет нарушения'}</span>`;
        let checkedDisplay = rec.checkedBy ? `<b>${escapeHtml(rec.checkedBy)}</b>` : '-';
        let statusCell = '';
        if (isAdmin) {
            let statusContent = '';
            if (rec.statusApproved) statusContent += '<div class="status-badge-arch yes"><i class="fa-solid fa-check"></i></div>';
            if (rec.statusRejected) statusContent += '<div class="status-badge-arch no"><i class="fa-solid fa-xmark"></i></div>';
            let statusHTML = statusContent ? `<div style="display:flex; gap:5px; justify-content:center;">${statusContent}</div>` : '<span style="color:#555;">-</span>';
            statusCell = `<td style="text-align:center;">${statusHTML}</td>`;
        }
        
        // --- ОБНОВЛЕННЫЙ HTML ДЛЯ АРХИВА (MOBILE SUPPORT) ---
        html += `<tr>
            <td data-label="ID">${rec.gameId}</td>
            <td data-label="Владелец">${escapeHtml(rec.owner)}</td>
            <td data-label="Название">${escapeHtml(rec.name) || '-'}</td>
            <td data-label="Нарушение">${violDisplay}</td>
            <td data-label="Доказательства">${proofDisplay}</td>
            <td data-label="Кто проверил">${checkedDisplay}</td>
            <td data-label="Ответственный">${escapeHtml(rec.addedBy)}</td>
            ${statusCell ? `<td data-label="Статус" style="text-align:center;">${statusCell.replace('<td style="text-align:center;">', '').replace('</td>', '')}</td>` : ''}
        </tr>`;
    });
    html += `</tbody></table></div>`;
    container.innerHTML = html;
}

function startTableListener(category) {
    const container = document.getElementById(`table-container-${category}`);
    recordsListenerRef = ref(db, `records/${category}`);
    get(ref(db, 'workers')).then(snap => { if(snap.exists()) workersDataCache = snap.val(); });
    onValue(recordsListenerRef, (snapshot) => {
        if(isDragging) return;
        if (!snapshot.exists()) { container.innerHTML = '<div class="empty-state">Список пуст</div>'; document.getElementById('pagination-container').classList.add('hidden'); allRecords = []; currentRecords = []; return; }
        const data = snapshot.val();
        allRecords = Object.entries(data).map(([key, val]) => ({ key, ...val }));
        allRecords.sort((a, b) => (parseInt(a.gameId) || 0) - (parseInt(b.gameId) || 0));
        applyFilters(true);
    });
}

window.applyFilters = (keepPage = false) => {
    const searchInput = document.getElementById('table-search-input').value.toLowerCase();
    const onlyUnchecked = document.getElementById('filter-unchecked').checked;
    const onlyCheckedWithViolation = document.getElementById('filter-checked-only').checked;

    currentRecords = allRecords.filter(rec => {
        let matchesSearch = true;
        if (searchInput) {
            const idMatch = rec.gameId.toString().includes(searchInput);
            const ownerMatch = rec.owner && rec.owner.toLowerCase().includes(searchInput);
            const nameMatch = rec.name && rec.name.toLowerCase().includes(searchInput);
            matchesSearch = idMatch || ownerMatch || nameMatch;
        }
        
        let matchesFilter = true;
        if (onlyUnchecked) {
             if (rec.checkedBy) matchesFilter = false;
        }
        if (onlyCheckedWithViolation) {
            const hasViolation = rec.violation && rec.violation !== 'Нет нарушения';
            const hasProof = rec.proof && rec.proof.trim().length > 0;
            if (!hasViolation && !hasProof) {
                matchesFilter = false;
            }
        }
        return matchesSearch && matchesFilter;
    });

    if (!keepPage) {
        currentPage = 1;
    } else {
        const totalPages = Math.ceil(currentRecords.length / itemsPerPage) || 1;
        if (currentPage > totalPages) currentPage = totalPages;
    }
    renderCurrentPage();
}

function getLvlClass(nick) {
    if (!nick || !workersDataCache) return '';
    let worker = workersDataCache[nick];
    if (!worker) {
        const lowerNick = nick.toLowerCase();
        const foundKey = Object.keys(workersDataCache).find(key => key.toLowerCase() === lowerNick);
        if (foundKey) worker = workersDataCache[foundKey];
    }
    if (!worker) return ''; 
    const lvl = parseInt(worker.lvl) || 0;
    if (lvl === 1 || lvl === 2) return 'lvl-1-2';
    if (lvl === 3) return 'lvl-3';
    if (lvl === 4) return 'lvl-4';
    if (lvl === 5) return 'lvl-5';
    return '';
}

function generateViolationSelect(currentVal, key) {
    let colorClass = '';
    if (currentVal === 'Нет нарушения') colorClass = 'viol-gray'; else if (currentVal === 'Маппинг') colorClass = 'viol-blue'; else if (currentVal === 'Названия') colorClass = 'viol-red'; else if (currentVal === 'Маппинг + названия') colorClass = 'viol-gradient'; 
    const rec = allRecords.find(r => r.key === key);
    let disabled = hasAccess ? '' : 'disabled'; 
    if (isRecordLocked(rec)) disabled = 'disabled';
    return `<select onchange="saveViolationDirect(this, '${key}')" class="custom-select ${colorClass}" ${disabled}><option value="Нет нарушения" ${currentVal === 'Нет нарушения' ? 'selected' : ''}>Нет нарушения</option><option value="Маппинг" ${currentVal === 'Маппинг' ? 'selected' : ''}>Маппинг</option><option value="Названия" ${currentVal === 'Названия' ? 'selected' : ''}>Названия</option><option value="Маппинг + названия" ${currentVal === 'Маппинг + названия' ? 'selected' : ''}>Маппинг + названия</option></select>`;
}

window.saveViolationDirect = (selectElement, key) => {
    const newVal = selectElement.value;
    selectElement.classList.remove('viol-gray', 'viol-blue', 'viol-red', 'viol-white', 'viol-gradient');
    if (newVal === 'Нет нарушения') selectElement.classList.add('viol-gray'); else if (newVal === 'Маппинг') selectElement.classList.add('viol-blue'); else if (newVal === 'Названия') selectElement.classList.add('viol-red'); else if (newVal === 'Маппинг + названия') selectElement.classList.add('viol-gradient'); 
    
    const rec = allRecords.find(r => r.key === key);
    if (isRecordLocked(rec)) {
        alert("Ошибка прав доступа: запись проверена другим сотрудником");
        applyFilters(true);
        return;
    }

    const oldVal = rec ? (rec.violation || "Нет нарушения") : "Нет нарушения";
    const updates = {}; 
    updates[`records/${currentCategory}/${key}/violation`] = newVal.trim(); 
    update(ref(db), updates).catch(error => { console.error("Ошибка сохранения: " + error.message); }); 
    
    let logMsg = `${currentUser.nickname} изменил Тип нарушения было "${oldVal}" Стало "${newVal}"`;
    if (rec && rec.checkedBy && rec.checkedBy !== currentUser.nickname) {
        logMsg = `${currentUser.nickname} изменил (${rec.checkedBy}) Тип нарушения было "${oldVal}" Стало "${newVal}"`;
    }
    logAction("EDIT", logMsg);
}

// === НОВОЕ: ВВОД СТРАНИЦЫ ВРУЧНУЮ ===
window.jumpToPage = (val) => {
    const pageNum = parseInt(val);
    const totalPages = Math.ceil(currentRecords.length / itemsPerPage) || 1;
    if (pageNum >= 1 && pageNum <= totalPages) {
        currentPage = pageNum;
        renderCurrentPage();
    } else {
        alert(`Введите число от 1 до ${totalPages}`);
    }
}

window.renderCurrentPage = () => {
    const container = document.getElementById(`table-container-${currentCategory}`);
    const paginationBox = document.getElementById('pagination-container');
    if(currentRecords.length === 0) { container.innerHTML = '<div class="empty-state">Ничего не найдено</div>'; renderPagination(0, paginationBox); return; }
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const pageRecords = currentRecords.slice(startIndex, endIndex);
    const totalPages = Math.ceil(currentRecords.length / itemsPerPage);
    const isAdmin = currentUser.role === 'admin';
    const adminTh = isAdmin ? `<th>Управление</th>` : '';
    const genButton = hasAccess ? `<button class="header-action-btn" title="Генерация команд" onclick="openGenChoice()"><i class="fa-solid fa-list-check"></i></button>` : '';
    const eraserClass = isEraserMode ? 'header-action-btn eraser-btn-active' : 'header-action-btn';
    const eraserButton = hasAccess ? `<button class="${eraserClass}" title="Режим удаления (Ластик)" onclick="toggleEraserMode()"><i class="fa-solid fa-eraser"></i></button>` : '';
    
    let html = `<div class="table-responsive"><table class="custom-table" id="main-table"><thead><tr><th style="width: 50px;">ID</th><th>Владелец</th><th>Название</th><th style="width: 200px;">Тип нарушения</th><th>Доказательства</th><th>Кто проверил? ${eraserButton}</th><th>Ответственный</th><th>Статус ${genButton}</th>${adminTh}</tr></thead><tbody>`;
    
    let workerData = workersDataCache[currentUser.nickname] || {};
    let position = workerData.position || "";
    let isManagement = isAdmin || ['ЗГС Маппинга', 'ГС Маппинга', 'Куратор'].includes(position);

    pageRecords.forEach(rec => {
        let proofDisplay = '-';
        let rawProof = rec.proof || '';
        const safeProof = escapeHtml(rawProof);
        const safeOwner = escapeHtml(rec.owner || '');
        const safeName = escapeHtml(rec.name || '');

        if (rawProof.length > 0) {
            const urlRegex = /(https?:\/\/[^\s]+)/g;
            if (rawProof.match(urlRegex)) {
                if (rawProof.match(/^https?:\/\/[^\s]+$/)) {
                     proofDisplay = `<a href="${safeProof}" target="_blank" class="proof-link" onclick="event.stopPropagation()"><i class="fa-solid fa-link"></i> Ссылка</a>`;
                } else {
                    proofDisplay = safeProof.replace(urlRegex, (url) => 
                        `<a href="${url}" target="_blank" class="proof-link-inline" onclick="event.stopPropagation()">${url}</a>`
                    );
                }
            } else {
                proofDisplay = safeProof;
            }
        } else { 
            proofDisplay = '<span style="color:#555; font-style:italic; font-size:0.8rem;">нет</span>'; 
        }

        const safeOwnerAttr = (rec.owner || '').replace(/"/g, '&quot;').replace(/'/g, "\\'");
        const safeNameAttr = (rec.name || '').replace(/"/g, '&quot;').replace(/'/g, "\\'");
        const safeProofAttr = (rawProof).replace(/"/g, '&quot;').replace(/'/g, "\\'");

        const editAttrOwner = hasAccess ? `class="editable-cell" onclick="editCell(this, '${currentCategory}', '${rec.key}', 'owner', '${safeOwnerAttr}')"` : '';
        const editAttrName = hasAccess ? `class="editable-cell" onclick="editCell(this, '${currentCategory}', '${rec.key}', 'name', '${safeNameAttr}')"` : '';
        const violSelectHTML = generateViolationSelect(rec.violation || 'Нет нарушения', rec.key);
        const editAttrProof = hasAccess ? `class="editable-cell" onclick="editCell(this, '${currentCategory}', '${rec.key}', 'proof', '${safeProofAttr}')"` : '';
        
        let checkedByDisplay = '';
        if (rec.checkedBy) { const lvlClass = getLvlClass(rec.checkedBy); checkedByDisplay = `<span class="${lvlClass}">${escapeHtml(rec.checkedBy)}</span>`; } else { checkedByDisplay = `<button class="take-btn" onclick="claimRecord('${currentCategory}', '${rec.key}')">Взять</button>`; }
        const dragAttrs = hasAccess ? `class="check-cell" onmousedown="handleMouseDown(this, '${rec.checkedBy || ''}', '${rec.key}')" onmouseenter="handleMouseEnter(this, '${rec.key}')"` : '';
        
        let actions = `<span style="margin-left: 10px;">`;
        if (isManagement) { 
            actions += `<button class="tbl-btn btn-del" style="display:inline-flex; width:24px; height:24px;" onclick="deleteRecord('${currentCategory}', '${rec.key}')"><i class="fa-solid fa-trash" style="font-size:0.7rem;"></i></button>`; 
        }
        actions += `</span>`;
        
        const responsibleDisplay = `<div style="display:flex; align-items:center; justify-content:space-between;"><div class="worker-nick-clickable" style="display:inline-flex;"><i class="fa-solid fa-user-check" style="margin-right:5px; color:#aaa;"></i>${escapeHtml(rec.addedBy)}</div>${actions}</div>`;
        const isApproved = rec.statusApproved === true; const isRejected = rec.statusRejected === true;
        
        let statusHTML = '';
        if (hasAccess) {
            const checkClass = isApproved ? 'status-btn active-yes' : 'status-btn'; const crossClass = isRejected ? 'status-btn active-no' : 'status-btn';
            statusHTML = `<div class="status-actions"><button class="${checkClass}" onclick="toggleStatus('${currentCategory}', '${rec.key}', 'statusApproved')"><i class="fa-solid fa-check"></i></button><button class="${crossClass}" onclick="toggleStatus('${currentCategory}', '${rec.key}', 'statusRejected')"><i class="fa-solid fa-xmark"></i></button></div>`;
        } else { if (isApproved) statusHTML += '<i class="fa-solid fa-check" style="color: #00ff88; font-size:1.2rem; margin-right:5px;"></i>'; if (isRejected) statusHTML += '<i class="fa-solid fa-xmark" style="color: #ff4757; font-size:1.2rem;"></i>'; if (!isApproved && !isRejected) statusHTML = '<span style="color:#555;">-</span>'; }
        
        let adminTd = ''; if (isAdmin) { const recordJson = JSON.stringify(rec).replace(/'/g, "&#39;"); adminTd = `<td><button class="tbl-btn btn-edit-admin" onclick='openEditRecord(${recordJson})'><i class="fa-solid fa-pencil"></i></button></td>`; }
        
        // --- ОБНОВЛЕННЫЙ HTML ДЛЯ ОСНОВНОЙ ТАБЛИЦЫ (MOBILE SUPPORT) ---
        html += `<tr>
            <td data-label="ID">${rec.gameId}</td>
            <td data-label="Владелец" ${editAttrOwner}>${safeOwner || '-'}</td>
            <td data-label="Название" ${editAttrName}>${safeName || '-'}</td>
            <td data-label="Нарушение">${violSelectHTML}</td>
            <td data-label="Доказательства" ${editAttrProof}>${proofDisplay}</td>
            <td data-label="Кто проверил" ${dragAttrs} data-key="${rec.key}">${checkedByDisplay}</td>
            <td data-label="Ответственный">${responsibleDisplay}</td>
            <td data-label="Статус" style="text-align:center;">${statusHTML}</td>
            ${isAdmin ? `<td data-label="Управление">${adminTd.replace('<td>', '').replace('</td>', '')}</td>` : ''}
        </tr>`;
    });
    html += `</tbody></table></div>`;
    container.innerHTML = html;
    renderPagination(totalPages, paginationBox);
}

window.openGenChoice = () => {
    const container = document.getElementById('gen-buttons-container');
    container.innerHTML = ''; 
    const btnNotif = document.createElement('button');
    btnNotif.className = 'gen-btn'; btnNotif.innerText = 'Notif'; btnNotif.onclick = () => generateCommands('notif'); container.appendChild(btnNotif);
    if (currentCategory === 'houses') { const btnDel = document.createElement('button'); btnDel.className = 'gen-btn'; btnDel.innerText = 'Delhname'; btnDel.onclick = () => generateCommands('delhname'); container.appendChild(btnDel); } 
    else if (currentCategory === 'biz') { const btnDel = document.createElement('button'); btnDel.className = 'gen-btn'; btnDel.innerText = 'Delbname'; btnDel.onclick = () => generateCommands('delbname'); container.appendChild(btnDel); }
    document.getElementById('gen-choice-modal').classList.remove('hidden');
}

window.closeGenChoice = () => document.getElementById('gen-choice-modal').classList.add('hidden');
window.generateCommands = (type) => {
    let targets = [];
    if (type === 'notif') { targets = currentRecords.filter(r => r.statusApproved === true); if (targets.length === 0) return alert("Нет записей с 'Галочкой' (Approved)!"); } 
    else { targets = currentRecords.filter(r => r.statusRejected === true); if (targets.length === 0) return alert("Нет записей с 'Крестиком' (Rejected)!"); }
    let resultText = "";
    targets.forEach(r => {
        const id = r.gameId; const nick = r.owner;
        if (type === 'delhname') resultText += `/delhname ${id}\n`; else if (type === 'delbname') resultText += `/delbname ${id}\n`;
        else if (type === 'notif') { let typeText = "имущества"; if (currentCategory === 'houses') typeText = "дома"; else if (currentCategory === 'biz') typeText = "бизнеса"; else if (currentCategory === 'trailers') typeText = "трейлера"; resultText += `/notif ${nick} Уберите маппинг у ${typeText} № ${id}, который нарушает правила установки объектов\n/notif ${nick} Если вы не уберете нарушающий маппинг в течение 24ч, то он будет удален\n/notif ${nick} Для ознакомления с правилами: /help - Частые вопросы - Правила кастомизации имущества\n`; }
    });
    document.getElementById('gen-result-text').value = resultText; closeGenChoice(); document.getElementById('gen-result-modal').classList.remove('hidden');
}

window.closeGenResult = () => document.getElementById('gen-result-modal').classList.add('hidden');
window.copyGenResult = () => { const text = document.getElementById('gen-result-text'); text.select(); document.execCommand('copy'); alert("Скопировано!"); }
window.renderPagination = (totalPages, container) => { if (totalPages <= 1) { container.classList.add('hidden'); return; } container.classList.remove('hidden'); container.innerHTML = `<button class="page-btn" onclick="changePage(-1)" ${currentPage === 1 ? 'disabled' : ''}>❮</button><span class="page-info">Стр. ${currentPage} из ${totalPages}</span><input type="number" class="page-jump-input" placeholder="..." onchange="jumpToPage(this.value)"><button class="page-btn" onclick="changePage(1)" ${currentPage === totalPages ? 'disabled' : ''}>❯</button>`; }

window.changePage = (delta) => { 
    currentPage += delta; 
    renderCurrentPage(); 
}

async function incrementWorkerTotal(nickname, amount = 1) { if(!nickname) return; const statsRef = ref(db, `workers/${nickname}/totalChecked`); try { await runTransaction(statsRef, (currentValue) => { return (currentValue || 0) + amount; }); } catch (e) { console.error("Ошибка обновления статистики:", e); } }

window.claimRecord = async (cat, key) => { 
    const snap = await get(ref(db, `records/${cat}/${key}/checkedBy`));
    if (snap.exists() && snap.val()) {
        alert("Эта запись уже занята!");
        return;
    }

    update(ref(db, `records/${cat}/${key}`), { checkedBy: currentUser.nickname }); 
    incrementWorkerTotal(currentUser.nickname, 1); 
    
    const rec = allRecords.find(r => r.key === key); 
    const typeName = translateCategory(cat);
    const id = rec ? rec.gameId : '?';
    
    logAction("CLAIM", `${currentUser.nickname} взял "${typeName}" "${id}"`); 
}

window.toggleEraserMode = () => { isEraserMode = !isEraserMode; renderCurrentPage(); }
window.addEventListener('mouseup', handleMouseUp);
window.handleMouseDown = (element, currentVal, key) => { if (!isEraserMode && !currentVal) return; isDragging = true; dragValue = isEraserMode ? null : currentVal; dragSelection = []; element.classList.add('drag-selected'); dragSelection.push(key); }
window.handleMouseEnter = (element, key) => { if (isDragging) { const record = currentRecords.find(r => r.key === key); if (!isEraserMode && record && record.checkedBy) return; element.classList.add('drag-selected'); dragSelection.push(key); } }

function handleMouseUp() { 
    if (!isDragging) return; 
    let changedCount = 0; 
    let blockedCount = 0;

    if (dragSelection.length > 0) { 
        const updates = {}; 
        dragSelection.forEach(key => { 
            const rec = currentRecords.find(r => r.key === key); 
            
            let isManager = false;
            if(currentUser.role === 'admin') isManager = true;
            else {
                 const w = workersDataCache[currentUser.nickname];
                 if(w && ['ЗГС Маппинга', 'ГС Маппинга', 'Куратор'].includes(w.position)) isManager = true;
            }

            if (rec && rec.checkedBy && rec.checkedBy !== currentUser.nickname && !isManager) {
                blockedCount++;
                return;
            }

            if (!isEraserMode && rec && rec.checkedBy !== dragValue) changedCount++; 
            if (rec) rec.checkedBy = dragValue; 
            updates[`records/${currentCategory}/${key}/checkedBy`] = dragValue; 
        }); 
        
        renderCurrentPage(); 
        update(ref(db), updates).catch(err => console.error(err)); 
        
        if (!isEraserMode && changedCount > 0) incrementWorkerTotal(currentUser.nickname, changedCount); 
        
        if (blockedCount > 0) {
            alert(`Не удалось изменить ${blockedCount} записей: они проверены другими сотрудниками.`);
        }
    } 
    isDragging = false; 
    dragSelection = []; 
    dragValue = null; 
    document.querySelectorAll('.drag-selected').forEach(el => el.classList.remove('drag-selected')); 
}

window.toggleStatus = async (category, key, field) => { const path = `records/${category}/${key}/${field}`; const snap = await get(ref(db, path)); const val = snap.val(); const newVal = !val; update(ref(db, `records/${category}/${key}`), { [field]: newVal }); const rec = currentRecords.find(r => r.key === key); if(newVal) logAction("STATUS", `Установил статус ${field} для ID ${rec ? rec.gameId : '?'}`); }

window.editCell = (element, category, key, field, originalValue) => { 
    const rec = currentRecords.find(r => r.key === key);
    
    if (isRecordLocked(rec)) {
         alert("Вы не можете редактировать запись, проверенную другим сотрудником!");
         return;
    }

    if (element.querySelector('input') || element.querySelector('select')) return; 
    if (activeInput && activeInput.parentNode !== element) activeInput.blur(); 
    let input = document.createElement('input'); 
    input.type = 'text'; 
    input.value = originalValue; 
    input.className = 'editing-input'; 
    activeInput = input; 
    input.onblur = () => { 
        const newVal = input.value; 
        if (newVal.startsWith('http')) element.innerHTML = `<a href="${newVal}" target="_blank" class="proof-link" onclick="event.stopPropagation()"><i class="fa-solid fa-link"></i> Ссылка</a>`; 
        else element.innerText = newVal; 
        if (activeInput === input) activeInput = null; 
        if (newVal !== originalValue) saveCell(category, key, field, newVal, originalValue); 
    }; 
    input.onkeydown = (e) => { if (e.key === 'Enter') input.blur(); }; 
    element.innerHTML = ''; 
    element.appendChild(input); 
    input.focus(); 
}

window.saveCell = (category, key, field, newValue, oldValue) => { 
    const updates = {}; 
    updates[`records/${category}/${key}/${field}`] = newValue.trim(); 
    update(ref(db), updates).catch(error => { console.error("Ошибка сохранения: " + error.message); }); 
    const rec = currentRecords.find(r => r.key === key); 
    
    if (field === 'proof') {
        logAction("EDIT", `${currentUser.nickname} изменил Доказательства у ID ${rec ? rec.gameId : '?'} с "${oldValue}" на "${newValue}"`);
    } else {
        logAction("EDIT", `Изменил ${field} у ID ${rec ? rec.gameId : '?'}`); 
    }
}

window.openUploadChoice = () => { 
    let workerData = workersDataCache[currentUser.nickname] || {};
    let position = workerData.position || "";
    let isManagement = currentUser.role === 'admin' || ['ЗГС Маппинга', 'ГС Маппинга', 'Куратор'].includes(position);
    if (!isManagement) { alert("У вас нет прав для загрузки данных!"); return; } 
    document.getElementById('upload-choice-modal').classList.remove('hidden'); 
}
window.closeUploadChoice = () => { document.getElementById('upload-choice-modal').classList.add('hidden'); }
window.openUploadInput = (cat) => { uploadCategory = cat; document.getElementById('upload-choice-modal').classList.add('hidden'); document.getElementById('upload-textarea').value = ''; document.getElementById('upload-input-modal').classList.remove('hidden'); }
window.closeUploadInput = () => { document.getElementById('upload-input-modal').classList.add('hidden'); uploadCategory = ''; }
window.processBatchUpload = async () => { const text = document.getElementById('upload-textarea').value.trim(); if (!text) return alert("Поле пустое!"); if (!uploadCategory) return alert("Ошибка категории!"); const lines = text.split('\n'); const updates = {}; const parsedMap = new Map(); let minId = Infinity; let maxId = -Infinity; const regex = /Название:\s*(.*?)\s*\((\d+)\),\s*Владелец:\s*(.*)/; lines.forEach(line => { line = line.trim(); if (!line) return; const match = line.match(regex); if (match) { const name = match[1].trim(); const id = parseInt(match[2].trim()); const owner = match[3].trim(); if (!isNaN(id)) { parsedMap.set(id, { name, owner }); if (id < minId) minId = id; if (id > maxId) maxId = id; } } }); if (parsedMap.size === 0) return alert("Не удалось распознать строки. Проверьте формат!"); let count = 0; const timestampBase = Date.now(); for (let i = minId; i <= maxId; i++) { const firebaseKey = timestampBase + count; let recordData = {}; if (parsedMap.has(i)) { const data = parsedMap.get(i); recordData = { gameId: i, owner: data.owner, name: data.name, violation: "Нет нарушения", proof: "", addedBy: currentUser.nickname || "System", status: 'pending', timestamp: Date.now() }; } else { recordData = { gameId: i, owner: "Скрипт пропустил заполни сам!", name: "Скрипт пропустил заполни сам!", violation: "Нет нарушения", proof: "", addedBy: currentUser.nickname || "System", status: 'pending', timestamp: Date.now() }; } updates[`records/${uploadCategory}/${firebaseKey}`] = recordData; count++; } try { await update(ref(db), updates); alert(`Успешно обработано. Загружено записей: ${count}`); closeUploadInput(); logAction("UPLOAD", `Загрузил ${count} записей в ${uploadCategory}`); } catch (e) { alert("Ошибка при загрузке: " + e.message); } }
window.openEditRecord = (record) => { document.getElementById('edit-key').value = record.key; document.getElementById('edit-owner').value = record.owner; document.getElementById('edit-name').value = record.name || ''; document.getElementById('edit-violation').value = record.violation || 'Нет нарушения'; document.getElementById('edit-proof').value = record.proof || ''; document.getElementById('edit-checkedBy').value = record.checkedBy || ''; document.getElementById('edit-record-modal').classList.remove('hidden'); }
window.closeEditRecordModal = () => document.getElementById('edit-record-modal').classList.add('hidden');
window.saveEditedRecord = async () => { const key = document.getElementById('edit-key').value; const owner = document.getElementById('edit-owner').value.trim(); const name = document.getElementById('edit-name').value.trim(); const violation = document.getElementById('edit-violation').value.trim(); const proof = document.getElementById('edit-proof').value.trim(); const checkedBy = document.getElementById('edit-checkedBy').value.trim(); if (!key || !owner) return alert("Владелец не может быть пустым!"); try { const finalCheckedBy = checkedBy === '' ? null : checkedBy; await update(ref(db, `records/${currentCategory}/${key}`), { owner: owner, name: name, violation: violation, proof: proof, checkedBy: finalCheckedBy, editedBy: currentUser.nickname }); closeEditRecordModal(); logAction("EDIT", `Отредактировал запись (Admin)`); } catch (e) { alert("Ошибка при сохранении: " + e.message); } }
window.openChangeDateModal = () => { document.getElementById('admin-modal').classList.add('hidden'); document.getElementById('change-date-modal').classList.remove('hidden'); }
window.closeChangeDateModal = () => { document.getElementById('change-date-modal').classList.add('hidden'); document.getElementById('admin-modal').classList.remove('hidden'); }
window.saveNewDate = async () => { const newDate = document.getElementById('new-date-input').value.trim(); if (!newDate) return alert("Введите дату!"); try { await set(ref(db, 'system/dateRange'), newDate); closeChangeDateModal(); alert("Дата обновлена!"); } catch (e) { alert("Ошибка: " + e.message); } }
window.openClearDatabaseChoice = () => { document.getElementById('admin-modal').classList.add('hidden'); document.getElementById('clear-db-modal').classList.remove('hidden'); }
window.closeClearDatabaseChoice = () => { document.getElementById('clear-db-modal').classList.add('hidden'); document.getElementById('admin-modal').classList.remove('hidden'); }
window.clearDatabase = (cat) => { if(!confirm(`ВНИМАНИЕ! Вы собираетесь полностью удалить ВСЕ данные из раздела "${cat}".\nЭто действие нельзя отменить.\n\nПродолжить?`)) return; if(!confirm("Точно удалить? Данные пропадут навсегда.")) return; remove(ref(db, `records/${cat}`)).then(() => { alert("Таблица успешно очищена."); closeClearDatabaseChoice(); logAction("DELETE", `Очистил таблицу ${cat}`); }).catch((err) => { alert("Ошибка при удалении: " + err.message); }); }
window.openAddRecordModal = () => document.getElementById('add-record-modal').classList.remove('hidden');
window.closeAddRecordModal = () => document.getElementById('add-record-modal').classList.add('hidden');
window.saveRecord = async () => { const id = document.getElementById('rec-id').value.trim(); const owner = document.getElementById('rec-owner').value.trim(); const name = document.getElementById('rec-name').value.trim(); const violation = document.getElementById('rec-violation').value.trim(); const proof = document.getElementById('rec-proof').value.trim(); if (!id || !owner || !violation) return alert("Заполните основные поля!"); const newRecord = { gameId: id, owner: owner, name: name, violation: violation, proof: proof, addedBy: currentUser.nickname, status: 'pending', timestamp: Date.now() }; try { await set(ref(db, `records/${currentCategory}/${Date.now()}`), newRecord); document.getElementById('rec-id').value = ''; document.getElementById('rec-owner').value = ''; document.getElementById('rec-name').value = ''; document.getElementById('rec-violation').value = 'Нет нарушения'; document.getElementById('rec-proof').value = ''; closeAddRecordModal(); logAction("EDIT", `Добавил вручную запись ID ${id}`); } catch (e) { alert("Ошибка: " + e.message); } }

window.deleteRecord = (cat, key) => { 
    let workerData = workersDataCache[currentUser.nickname] || {};
    let position = workerData.position || "";
    let isManagement = currentUser.role === 'admin' || ['ЗГС Маппинга', 'ГС Маппинга', 'Куратор'].includes(position);
    
    if(!isManagement) return alert("У вас нет прав для удаления!");

    const rec = allRecords.find(r => r.key === key);
    if (isRecordLocked(rec)) {
         alert("Вы не можете удалить запись, проверенную другим сотрудником!");
         logAction("DELETE_ATTEMPT", `Попытка удалить чужую запись ID ${rec.gameId}`);
         return;
    }

    if(confirm("Удалить запись?")) { 
        if (rec && rec.checkedBy === currentUser.nickname) incrementWorkerTotal(currentUser.nickname, -1); 
        
        const typeName = translateCategory(cat);
        const id = rec ? rec.gameId : '?';
        
        let logMsg = `${currentUser.nickname} удалил "${typeName}" "${id}"`;
        if (rec && rec.checkedBy && rec.checkedBy !== currentUser.nickname) {
            logMsg = `${currentUser.nickname} удалил (${rec.checkedBy}) "${typeName}" "${id}"`;
        }

        remove(ref(db, `records/${cat}/${key}`)); 
        logAction("DELETE", logMsg); 
    } 
}

window.openWorkersModal = () => { document.getElementById('workers-modal').classList.remove('hidden'); startWorkersListListener(); showWorkersList(); }
window.closeWorkersModal = () => document.getElementById('workers-modal').classList.add('hidden');
window.showAddWorkerForm = () => { document.getElementById('workers-view-list').classList.add('hidden'); document.getElementById('workers-view-add').classList.remove('hidden'); }
window.showWorkersList = () => { document.getElementById('workers-view-add').classList.add('hidden'); document.getElementById('workers-view-list').classList.remove('hidden'); }

function startWorkersListListener() {
    if (workersListRef) return;
    const list = document.getElementById('workers-list');
    workersListRef = ref(db, 'workers');
    get(ref(db, 'users')).then(snap => { if(snap.exists()) { snap.forEach(u => { usersDataCache[u.val().nickname] = u.val(); }); } });

    if (statusListenerRef) off(statusListenerRef);
    let onlineData = {};
    statusListenerRef = ref(db, 'status');
    onValue(statusListenerRef, (snap) => {
        onlineData = snap.val() || {};
        renderWorkerList(); 
    });

    onValue(workersListRef, async (snapshot) => {
        workersDataCache = snapshot.val() || {}; 
        
        updateInterfaceAccess();
        if(currentCategory === 'archive') renderArchivePage();
        
        renderWorkerList();
    });

    function renderWorkerList() {
        if(!list) return;
        list.innerHTML = ''; 
        if(!workersDataCache || Object.keys(workersDataCache).length === 0) { list.innerHTML = '<p style="padding:20px; color:#aaa; text-align:center;">Список пуст</p>'; return; }
        
        let workers = Object.values(workersDataCache);
        workers.sort((a, b) => (a.order || 0) - (b.order || 0));
        
        workers.forEach(w => {
            if (!w.nickname) return;
            const item = document.createElement('div');
            item.className = 'worker-row';
            const userData = usersDataCache[w.nickname] || {};
            const avatarSrc = userData.avatar || DEFAULT_AVATAR;
            const lvlClass = getLvlClass(w.nickname);
            const statusObj = onlineData[w.nickname];
            const isOnline = statusObj && statusObj.state === 'online';
            const statusClass = isOnline ? 'online' : 'offline';
            
            let controls = '';
            if (currentUser.role === 'admin') { controls = `<div class="admin-controls-row"><button class="mini-btn" onclick="moveWorker('${w.nickname}', -1)">▲</button><button class="mini-btn" onclick="moveWorker('${w.nickname}', 1)">▼</button><button class="mini-btn del-mini" onclick="deleteWorker('${w.nickname}')">✖</button></div>`; }
            
            item.innerHTML = `
                <div class="worker-pos-cell">${w.position}</div>
                <div class="worker-nick-cell">
                    <div class="worker-nick-clickable" onclick="openWorkerProfile('${w.nickname}', '${w.position}')">
                        <div class="worker-status-dot ${statusClass}"></div>
                        <img src="${avatarSrc}" class="worker-avatar-small">
                        <span class="${lvlClass}">${w.nickname}</span>
                    </div>
                    ${controls}
                </div>`;
            list.appendChild(item);
        });
    }
}

window.openBaseUsersModal = () => { document.getElementById('base-users-modal').classList.remove('hidden'); loadBaseUsers(); }
window.closeBaseUsersModal = () => { document.getElementById('base-users-modal').classList.add('hidden'); }
async function loadBaseUsers() {
    const list = document.getElementById('base-users-list');
    list.innerHTML = '<div class="loader"></div>';
    try {
        const snapshot = await get(ref(db, 'users'));
        if (!snapshot.exists()) { list.innerHTML = '<p style="text-align:center; padding:20px; color:#aaa;">Нет пользователей</p>'; return; }
        list.innerHTML = '';
        snapshot.forEach(child => {
            const user = child.val();
            const item = document.createElement('div');
            item.className = 'worker-row';
            const avatar = user.avatar || DEFAULT_AVATAR;
            item.innerHTML = `<div class="worker-pos-cell" style="font-weight:normal; font-size:0.8rem; color:#888;">${user.vkId || 'Нет VK'} <br><span style="font-size:0.7rem;">${user.role || 'user'}</span></div><div class="worker-nick-cell"><img src="${avatar}" class="worker-avatar-small"><span>${user.nickname}</span></div>`;
            list.appendChild(item);
        });
    } catch (e) { list.innerHTML = '<p style="color:red; text-align:center;">Ошибка загрузки</p>'; console.error(e); }
}

window.openWorkerProfile = async (nick, position) => {
    const userData = usersDataCache[nick];
    document.getElementById('view-user-nick').innerText = nick;
    document.getElementById('view-user-role-badge').innerText = position || "Сотрудник";
    document.getElementById('view-user-avatar').src = (userData && userData.avatar) ? userData.avatar : DEFAULT_AVATAR;
    document.getElementById('view-user-vk').innerText = (userData && userData.vkId) ? userData.vkId : "Не указан";
    
    // ЛОГИКА ОТОБРАЖЕНИЯ СТАТУСА И РАМКИ
    const statusEl = document.getElementById('view-user-status');
    const avatarBox = document.querySelector('.mini-avatar-box');
    
    statusEl.innerText = "Offline";
    statusEl.className = "status-indicator-text offline";
    avatarBox.className = "mini-avatar-box offline";

    get(ref(db, `status/${nick}`)).then(snap => {
        if (snap.exists() && snap.val().state === 'online') {
            statusEl.innerText = "Online";
            statusEl.className = "status-indicator-text online";
            avatarBox.className = "mini-avatar-box online";
        }
    });

    document.getElementById('stats-current').innerText = '...';
    document.getElementById('stats-total').innerText = '...';
    document.getElementById('view-user-modal').classList.remove('hidden');
    let currentCount = 0;
    try {
        const recordsSnap = await get(ref(db, 'records'));
        if(recordsSnap.exists()) {
            const allCategories = recordsSnap.val();
            Object.values(allCategories).forEach(categoryObj => {
                if(categoryObj) { Object.values(categoryObj).forEach(rec => { if (rec.checkedBy === nick) currentCount++; }); }
            });
        }
        document.getElementById('stats-current').innerText = currentCount;
    } catch(e) { document.getElementById('stats-current').innerText = "Err"; }
    try {
        const workerSnap = await get(ref(db, `workers/${nick}/totalChecked`));
        let totalVal = workerSnap.exists() ? workerSnap.val() : 0;
        if (currentCount > totalVal) { totalVal = currentCount; update(ref(db, `workers/${nick}`), { totalChecked: totalVal }); }
        document.getElementById('stats-total').innerText = totalVal;
    } catch(e) { document.getElementById('stats-total').innerText = "Err"; }
}

window.closeViewUserModal = () => document.getElementById('view-user-modal').classList.add('hidden');
window.moveWorker = async (nick, direction) => {
    const snap = await get(ref(db, 'workers')); let workers = []; snap.forEach(c => { let val = c.val(); val.order = Number(val.order) || 0; workers.push(val); }); workers.sort((a, b) => a.order - b.order);
    const idx = workers.findIndex(w => w.nickname === nick); if (idx === -1 || (direction === -1 && idx === 0) || (direction === 1 && idx === workers.length - 1)) return;
    const otherIdx = idx + direction; const tempOrder = workers[idx].order; workers[idx].order = workers[otherIdx].order; workers[otherIdx].order = tempOrder;
    await update(ref(db, 'workers/' + workers[idx].nickname), { order: workers[idx].order }); await update(ref(db, 'workers/' + workers[otherIdx].nickname), { order: workers[otherIdx].order });
}
window.deleteWorker = (nick) => { if(confirm(`Удалить ${nick}?`)) remove(ref(db, 'workers/' + nick)); }
window.saveWorker = async () => {
    const nick = document.getElementById('worker-nick-input').value.trim(); 
    const pos = document.getElementById('worker-pos-input').value; // Берем из Select
    const lvl = document.getElementById('worker-lvl-input').value.trim() || 0;
    if(!nick || !pos) return alert("Заполните поля!");
    try { 
        const snap = await get(ref(db, 'workers')); let maxOrder = 0; snap.forEach(c => { const val = Number(c.val().order); if(!isNaN(val) && val > maxOrder) maxOrder = val; }); 
        await set(ref(db, 'workers/' + nick), { nickname: nick, position: pos, lvl: Number(lvl), order: maxOrder + 1, totalChecked: 0 }); 
        document.getElementById('worker-nick-input').value = ''; 
        document.getElementById('worker-lvl-input').value = ''; 
        showWorkersList(); 
    } catch (err) { alert("Ошибка: " + err.message); }
}

window.triggerFileUpload = () => document.getElementById('file-input').click();
window.prepAvatarCrop = () => {
    const file = document.getElementById('file-input').files[0]; if(!file) return; const reader = new FileReader();
    reader.onload = (e) => { const image = document.getElementById('image-to-crop'); image.src = e.target.result; document.getElementById('profile-modal').classList.add('hidden'); document.getElementById('crop-modal').classList.remove('hidden'); if(cropper) cropper.destroy(); cropper = new Cropper(image, { aspectRatio: 1, viewMode: 1, autoCropArea: 1 }); }; reader.readAsDataURL(file);
}
window.cancelCrop = () => { document.getElementById('crop-modal').classList.add('hidden'); document.getElementById('profile-modal').classList.remove('hidden'); if(cropper) { cropper.destroy(); cropper = null; } document.getElementById('file-input').value = ''; }
window.saveCroppedAvatar = () => { if(!cropper) return; const canvas = cropper.getCroppedCanvas({ width: 300, height: 300, fillColor: '#fff' }); const base64Image = canvas.toDataURL('image/jpeg', 0.8); if(auth.currentUser) { update(ref(db, 'users/' + auth.currentUser.uid), { avatar: base64Image }).then(() => { cancelCrop(); }); } }

window.openDeleteAvatarConfirm = () => document.getElementById('delete-avatar-modal').classList.remove('hidden');
window.closeDeleteAvatarModal = () => document.getElementById('delete-avatar-modal').classList.add('hidden');
window.confirmDeleteAvatar = async () => { if(auth.currentUser) { await update(ref(db, 'users/' + auth.currentUser.uid), { avatar: null }); closeDeleteAvatarModal(); updateProfileInfo(); } }

function monitorSystem() {
    onValue(ref(db, 'system/siteVersion'), (snap) => { const serverVer = snap.val(); if(currentSiteVersion === null) currentSiteVersion = serverVer; else if(serverVer !== currentSiteVersion) document.getElementById('update-popup').classList.remove('hidden'); });
    onValue(ref(db, 'system/maintenance'), (snap) => { isMaintenanceActive = snap.val() === true; const overlay = document.getElementById('maintenance-screen'); const adminBadge = document.getElementById('admin-maint-indicator'); if (isMaintenanceActive) { if (currentUser.role === 'admin') { overlay.classList.add('hidden'); adminBadge.classList.remove('hidden'); updateMaintButton(true); } else { overlay.classList.remove('hidden'); } } else { overlay.classList.add('hidden'); adminBadge.classList.add('hidden'); updateMaintButton(false); } });
    onValue(ref(db, 'system/dateRange'), (snap) => { const dateText = snap.val(); const el = document.getElementById('header-date-text'); if(el) el.innerText = dateText || "Установите дату"; });
}
window.pushSiteUpdate = () => { set(ref(db, 'system/siteVersion'), Date.now()).then(() => alert("Обновление отправлено!")); }
window.toggleMaintenance = () => { const newState = !isMaintenanceActive; set(ref(db, 'system/maintenance'), newState); }
function updateMaintButton(isActive) { const btn = document.getElementById('btn-toggle-maint'); if (isActive) { btn.innerHTML = '<i class="fa-solid fa-power-off"></i> Выключить Тех. Работы'; btn.classList.add('active'); } else { btn.innerHTML = '<i class="fa-solid fa-power-off"></i> Включить Тех. Работы'; btn.classList.remove('active'); } }

async function updateProfileInfo() { document.getElementById('profile-nick-big').innerText = currentUser.nickname; document.getElementById('profile-vk').innerText = currentUser.vkId; document.getElementById('profile-avatar-big').src = currentUser.avatar || DEFAULT_AVATAR; let roleText = "Нет должности"; try { const wSnap = await get(ref(db, 'workers/' + currentUser.nickname)); if (wSnap.exists()) roleText = wSnap.val().position; } catch(e) {} if (currentUser.role === 'admin') { if(roleText === "Нет должности") roleText = "Администратор"; else roleText += " / Админ"; } document.getElementById('profile-role').innerText = roleText; }
function startWatchingAccess(user) { if (workerListenerRef) off(workerListenerRef); if (user.role === 'admin') { grantAccess(); return; } workerListenerRef = ref(db, 'workers/' + user.nickname); onValue(workerListenerRef, (snapshot) => { if (snapshot.exists()) { if (!hasAccess) grantAccess(); } else { denyAccess(); } }); }
function grantAccess() { hasAccess = true; document.getElementById('tab-access').classList.add('hidden'); switchTab('houses'); }
function denyAccess() { hasAccess = false; document.getElementById('tab-access').classList.remove('hidden'); switchTab('access'); }

onAuthStateChanged(auth, (user) => {
    if (user) {
        if (userListenerRef) off(userListenerRef);
        userListenerRef = ref(db, 'users/' + user.uid);
        onValue(userListenerRef, (snapshot) => {
            if (snapshot.exists()) { 
                currentUser = snapshot.val(); 

                const vkError = validateVkId(currentUser.vkId);
                if (vkError) {
                    document.getElementById('fix-vk-modal').classList.remove('hidden');
                } else {
                    document.getElementById('fix-vk-modal').classList.add('hidden');
                }

                document.getElementById('auth-container').classList.add('hidden'); 
                document.getElementById('dashboard').classList.remove('hidden'); 
                document.getElementById('header-nickname').innerText = currentUser.nickname; 
                document.getElementById('header-avatar').src = currentUser.avatar || DEFAULT_AVATAR; 
                startWorkersListListener();
                startNotificationListener(); // ЗАПУСК СЛУШАТЕЛЯ УВЕДОМЛЕНИЙ
                updateInterfaceAccess();
                if (currentCategory && currentCategory !== 'archive') {
                    renderCurrentPage();
                } else if (currentCategory === 'archive') {
                    renderArchivePage();
                }
                startWatchingAccess(currentUser); 
                updateProfileInfo(); 
                monitorSystem(); 
                setupPresence(user.uid, currentUser.nickname);
            } 
            else { setTimeout(() => { get(ref(db, 'users/' + user.uid)).then(s => { if(!s.exists()) { alert("Аккаунт удален!"); logout(); } }); }, 1000); }
        });
    } else {
        if (userListenerRef) off(userListenerRef); if (workerListenerRef) off(workerListenerRef); if (workersListRef) off(workersListRef); workersListRef = null; if (recordsListenerRef) off(recordsListenerRef);
        document.getElementById('dashboard').classList.add('hidden'); document.getElementById('auth-container').classList.remove('hidden'); currentUser = {}; hasAccess = false;
    }
});

window.saveCorrectedVkId = async () => {
    const newVk = document.getElementById('fix-vk-input').value.trim();
    const error = validateVkId(newVk);
    if (error) return alert(error);
    
    try {
        await update(ref(db, 'users/' + auth.currentUser.uid), { vkId: newVk });
        alert("VK ID успешно обновлен!");
        document.getElementById('fix-vk-modal').classList.add('hidden');
    } catch(e) {
        alert("Ошибка: " + e.message);
    }
}

window.openProfile = () => { updateProfileInfo(); document.getElementById('profile-modal').classList.remove('hidden'); }
window.closeProfile = () => document.getElementById('profile-modal').classList.add('hidden');
window.openAdminModal = () => { document.getElementById('profile-modal').classList.add('hidden'); document.getElementById('admin-modal').classList.remove('hidden'); }
window.closeAdminModal = () => document.getElementById('admin-modal').classList.add('hidden');
window.closeAccessPopup = () => document.getElementById('access-denied-popup').classList.add('hidden');

window.openGenerateRangeModal = () => {
    document.getElementById('admin-modal').classList.add('hidden');
    document.getElementById('generate-range-modal').classList.remove('hidden');
}
window.closeGenerateRangeModal = () => {
     document.getElementById('generate-range-modal').classList.add('hidden');
     document.getElementById('admin-modal').classList.remove('hidden');
}
window.processRangeGeneration = async () => {
    const cat = document.getElementById('gen-range-cat').value;
    const start = parseInt(document.getElementById('gen-range-from').value);
    const end = parseInt(document.getElementById('gen-range-to').value);

    if (isNaN(start) || isNaN(end) || start > end) return alert("Некорректный диапазон!");
    if (!confirm(`Вы уверены? Будет создано ${end - start + 1} пустых записей в категории ${cat}.`)) return;

    const updates = {};
    const timestampBase = Date.now();
    let count = 0;

    for (let i = start; i <= end; i++) {
        const firebaseKey = timestampBase + count;
        updates[`records/${cat}/${firebaseKey}`] = {
            gameId: i,
            owner: "", 
            name: "",  
            violation: "Нет нарушения",
            proof: "",
            addedBy: currentUser.nickname,
            status: 'pending',
            timestamp: Date.now()
        };
        count++;
    }

    try {
        await update(ref(db), updates);
        alert(`Успешно создано ${count} пустых слотов!`);
        closeGenerateRangeModal();
        logAction("UPLOAD", `Сгенерировал пустой диапазон ID ${start}-${end} в ${cat}`);
    } catch (e) {
        alert("Ошибка: " + e.message);
    }
}