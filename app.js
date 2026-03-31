/* ══════════════════════════════════════════════════
   PharmaCare Pro — Complete App Logic
   ══════════════════════════════════════════════════ */
'use strict';

// ── State ────────────────────────────────────────────
let STATE = {
  settings: {
    storeName: 'My Pharmacy', storeType: 'Retail Pharmacy',
    address: '', phone: '', email: '', license: '',
    gstin: '', defaultGst: 12, currency: '₹',
    lowStockThreshold: 10, expiryAlertDays: 90,
    // Wholesale-specific
    supplierName: '', wholesaler: '', ownerName: '', wholesalerId: '',
    // Retail-specific
    shopName: '', retailerOwner: '',
    // QR Codes (base64)
    wholesaleUpiQr: '', retailUpiQr: ''
  },
  categories: [],
  products: [],
  stockIns: [],
  bills: [],          // all bills (tagged with billStoreType)
  credits: [],
  shopCredits: [],
  nextBillNo: 1,
  dashboardResets: {} // { wholesale: 'YYYY-MM-DD', retail: 'YYYY-MM-DD' }
};

// ── Global DOM helper (used throughout, incl. import/export modals) ──
const el = id => document.getElementById(id);

let billItems = [];
let selectedBillProduct = null;
let selectedStockInProduct = null;
let analysisPeriod = 7;
let expiryFilter = 'expired';
let chartInstances = {};

// ── Mobile Sidebar ───────────────────────────────────
function openSidebar() {
  document.getElementById('sidebar').classList.add('open');
  document.getElementById('sidebar-overlay').classList.remove('hidden');
  document.body.style.overflow = 'hidden';
}
function closeSidebar() {
  document.getElementById('sidebar').classList.remove('open');
  document.getElementById('sidebar-overlay').classList.add('hidden');
  document.body.style.overflow = '';
}
function isMobile() { return window.innerWidth <= 768; }

// ── Persist ──────────────────────────────────────────
// Detect if running via Flask (http/https) or as a local file
const IS_FLASK = window.location.protocol !== 'file:';

function saveState() {
  if (IS_FLASK) {
    // Push full state to Flask/SQLite
    fetch('/api/state', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(STATE)
    }).catch(e => console.error('[PharmaCare] Save to server failed:', e));
  } else {
    // Standalone file:// mode — use localStorage
    try { localStorage.setItem('pharmacare_v2', JSON.stringify(STATE)); } catch(e) {}
  }
}

async function loadState() {
  if (IS_FLASK) {
    // Load full state from Flask/SQLite
    try {
      const resp = await fetch('/api/state');
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const data = await resp.json();
      STATE = { ...STATE, ...data };
    } catch(e) { console.error('[PharmaCare] Load from server failed:', e); }
  } else {
    // Standalone file:// mode — use localStorage
    try { const d = localStorage.getItem('pharmacare_v2'); if (d) { const p = JSON.parse(d); STATE = { ...STATE, ...p }; } } catch(e) {}
  }
}

// ── Utilities ────────────────────────────────────────
function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2, 6); }
function cur(n) { return (STATE.settings.currency || '₹') + parseFloat(n || 0).toFixed(2); }
function today() { return new Date().toISOString().split('T')[0]; }
function thisMonth() { return new Date().toISOString().slice(0, 7); }
function fmtDate(d) { if (!d) return '—'; return new Date(d).toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' }); }
function fmtMonth(m) { if (!m) return '—'; const [y, mo] = m.split('-'); return new Date(y, mo-1).toLocaleDateString('en-IN', { month: 'short', year: 'numeric' }); }
function daysAgo(n) { const d = new Date(); d.setDate(d.getDate() - n); return d.toISOString().split('T')[0]; }

// Expiry helpers
function expiryDaysLeft(expiryMonth) {
  if (!expiryMonth) return 9999;
  const exp = new Date(expiryMonth + '-01');
  // last day of that month
  exp.setMonth(exp.getMonth() + 1); exp.setDate(0);
  return Math.ceil((exp - new Date()) / 86400000);
}
function getExpiryBadge(expiryMonth) {
  if (!expiryMonth) return { cls: 'badge-gray', label: 'No Expiry' };
  const days = expiryDaysLeft(expiryMonth);
  if (days < 0)   return { cls: 'badge-red',   label: 'Expired' };
  if (days <= 30) return { cls: 'badge-red',   label: `${days}d left` };
  if (days <= 90) return { cls: 'badge-amber', label: `${days}d left` };
  return { cls: 'badge-green', label: fmtMonth(expiryMonth) };
}

// Toast
function toast(msg, type = 'ok') {
  const el = document.getElementById('toast');
  if (!el) return;
  el.textContent = msg;
  el.className = `toast ${type}`;
  clearTimeout(el._t);
  el._t = setTimeout(() => el.classList.add('hidden'), 3200);
}
function openModal(id) { document.getElementById(id)?.classList.remove('hidden'); }
function closeModal(id) { document.getElementById(id)?.classList.add('hidden'); }
function destroyChart(key) { if (chartInstances[key]) { chartInstances[key].destroy(); delete chartInstances[key]; } }

// ── Page Meta ────────────────────────────────────────
const PAGE_META = {
  dashboard:  { title: 'Dashboard',       sub: 'Pharmacy overview at a glance',       action: '⟳ Reset Dashboard', fn: 'resetDashboard' },
  products:   { title: 'Inventory',        sub: 'Manage your medicine inventory',      action: '+ Add Medicine', fn: 'openProductModal' },
  'stock-in': { title: 'Stock Details',         sub: 'Record new purchases & arrivals',     action: '+ Add Medicine', fn: 'openProductModal' },
  billing:    { title: 'Billing',          sub: 'Create a new prescription bill',      action: '🖨 Print Last',  fn: 'printLastBill' },
  history:    { title: 'Sales History',     sub: 'All past transactions',               action: 'Export CSV',     fn: 'exportCSV' },
  credit:     { title: 'Credit',            sub: 'Amount Due / Pending Payments',       action: '+ Add Receipt',  fn: 'toggleCreditForm' },
  analysis:   { title: 'Sales Analysis',    sub: 'Performance insights & trends',       action: 'Export CSV',     fn: 'exportCSV' },
  expiry:     { title: 'Expiry Tracker',    sub: 'Monitor expiring medicines',          action: '+ Add Medicine', fn: 'openProductModal' },
  categories: { title: 'Categories',        sub: 'Manage medicine categories',          action: '+ Add Category', fn: 'focusCatName' },
  settings:   { title: 'Settings',          sub: 'Pharmacy configuration',              action: 'Save Settings',  fn: 'saveSettings' }
};

function navigate(pageId) {
  document.querySelectorAll('.nav-link').forEach(n => n.classList.toggle('active', n.dataset.page === pageId));
  document.querySelectorAll('.page').forEach(p => p.classList.toggle('active', p.id === 'page-' + pageId));
  document.querySelectorAll('.bnav-btn').forEach(b => b.classList.toggle('active', b.dataset.page === pageId));
  const meta = PAGE_META[pageId] || { title: pageId, sub: '', action: '' };
  document.getElementById('page-heading').textContent = meta.title;
  document.getElementById('page-sub').textContent = meta.sub;
  const btn = document.getElementById('topbar-action-btn');
  btn.textContent = meta.action;
  btn.onclick = meta.fn ? window[meta.fn] : () => {};
  btn.style.display = (pageId === 'settings') ? 'none' : '';
  if (isMobile()) closeSidebar();
  if (pageId === 'dashboard') renderDashboard();
  if (pageId === 'products') renderProducts();
  if (pageId === 'stock-in') renderStockInHistory();
  if (pageId === 'billing') { updateBillingLayout(); updateBillNo(); updateBillingQrPanel(); }
  if (pageId === 'history') renderHistory();
  if (pageId === 'credit') { checkPharmacyTypeCredit(); const type=(STATE.settings.storeType||'').trim(); if(type==='Wholesale Pharma') renderCreditTable(creditFilter); else renderRetailCreditTable(); }
  if (pageId === 'analysis') renderAnalysis();
  if (pageId === 'expiry') renderExpiryTracker(expiryFilter);
  if (pageId === 'categories') renderCategories();
  if (pageId === 'settings') loadSettingsForm();
}
function handleTopbarAction() {
  const pageId = document.querySelector('.nav-link.active')?.dataset.page;
  const meta = PAGE_META[pageId];
  if (meta?.fn && window[meta.fn]) window[meta.fn]();
}

// ── Init ─────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  await loadState();
  if (STATE.categories.length === 0) seedData();

  document.querySelectorAll('.nav-link').forEach(el => el.addEventListener('click', e => { e.preventDefault(); navigate(el.dataset.page); }));
  document.querySelectorAll('[data-page]').forEach(el => {
    if (!el.classList.contains('nav-link') && !el.classList.contains('bnav-btn'))
      el.addEventListener('click', e => { e.preventDefault(); navigate(el.dataset.page); });
  });
  document.querySelectorAll('.bnav-btn').forEach(btn => btn.addEventListener('click', () => navigate(btn.dataset.page)));

  // Period buttons
  document.querySelectorAll('.period-btn[data-period]').forEach(btn => {
    btn.addEventListener('click', () => {
      btn.closest('.period-btns, .page').querySelectorAll('.period-btn[data-period]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active'); analysisPeriod = parseInt(btn.dataset.period); renderAnalysis();
    });
  });
  // Expiry filter buttons
  document.querySelectorAll('.period-btn[data-expiry]').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.period-btn[data-expiry]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active'); expiryFilter = btn.dataset.expiry; renderExpiryTracker(expiryFilter);
    });
  });

  const bd = document.getElementById('bill-date'); if (bd) bd.value = today();
  const fd = document.getElementById('footer-date'); if (fd) fd.textContent = new Date().toLocaleDateString('en-IN', { day:'numeric', month:'short', year:'numeric' });

  document.addEventListener('click', e => {
    if (!e.target.closest('#bill-prod-search') && !e.target.closest('#bill-dropdown'))
      document.getElementById('bill-dropdown')?.classList.add('hidden');
    if (!e.target.closest('#si-prod-search') && !e.target.closest('#si-dropdown'))
      document.getElementById('si-dropdown')?.classList.add('hidden');
  });

  applyBranding();
  await loadDashboardResets();
  renderDashboard();
  populateCategorySelects();
  checkPharmacyTypeCredit();
  updateBillingLayout();
  updateBillingQrPanel();
});

// ── Seed Data ────────────────────────────────────────
function seedData() {
  STATE.categories = [
    { id: uid(), name: 'Analgesics', desc: 'Pain relievers' },
    { id: uid(), name: 'Antibiotics', desc: 'Antibacterial medicines' },
    { id: uid(), name: 'Antacids', desc: 'Stomach & digestion' },
    { id: uid(), name: 'Antihistamines', desc: 'Allergy medicines' },
    { id: uid(), name: 'Vitamins & Supplements', desc: 'Nutritional supplements' },
    { id: uid(), name: 'Antidiabetics', desc: 'Diabetes medicines' },
    { id: uid(), name: 'Cardiovascular', desc: 'Heart & BP medicines' },
    { id: uid(), name: 'Syrups & Liquids', desc: 'Liquid medicines' },
    { id: uid(), name: 'Topical', desc: 'Creams, gels & ointments' },
  ];
  const c = STATE.categories;
  const mo = (n) => { const d = new Date(); d.setMonth(d.getMonth() + n); return d.toISOString().slice(0,7); };

  STATE.products = [
    { id:uid(), name:'Paracetamol 500mg', category:c[0].id, unit:'Tablet', purchase:12, sale:22, gst:5, stock:200, minStock:50, sku:'B240101', expiry:mo(18), brand:'Cipla', desc:'Paracetamol 500mg', hsn:'30049099' },
    { id:uid(), name:'Amoxicillin 250mg', category:c[1].id, unit:'Capsule', purchase:55, sale:85, gst:12, stock:8, minStock:20, sku:'SP2024A', expiry:mo(10), brand:'Sun Pharma', desc:'Amoxicillin trihydrate 250mg', hsn:'30041090' },
    { id:uid(), name:'Azithromycin 500mg', category:c[1].id, unit:'Tablet', purchase:78, sale:120, gst:12, stock:45, minStock:20, sku:'LU4422', expiry:mo(2), brand:'Lupin', desc:'Azithromycin 500mg', hsn:'30041090' },
    { id:uid(), name:'Cetirizine 10mg', category:c[3].id, unit:'Tablet', purchase:18, sale:35, gst:5, stock:150, minStock:30, sku:'MK0001', expiry:mo(24), brand:'Mankind Pharma', desc:'Cetirizine HCl 10mg', hsn:'30049099' },
    { id:uid(), name:'Omeprazole 20mg', category:c[2].id, unit:'Capsule', purchase:30, sale:55, gst:12, stock:4, minStock:25, sku:'DR7788', expiry:mo(8), brand:"Dr. Reddy's", desc:'Omeprazole 20mg', hsn:'30049099' },
    { id:uid(), name:'Cough Syrup 100ml', category:c[7].id, unit:'Bottle', purchase:40, sale:65, gst:12, stock:5, minStock:15, sku:'PF990X', expiry:mo(6), brand:'Pfizer', desc:'Dextromethorphan + Guaifenesin', hsn:'30049039' },
    { id:uid(), name:'Metformin 500mg', category:c[5].id, unit:'Tablet', purchase:20, sale:42, gst:5, stock:300, minStock:100, sku:'USV0055', expiry:mo(16), brand:'USV Ltd', desc:'Metformin HCl 500mg', hsn:'30049099' },
    { id:uid(), name:'Amlodipine 5mg', category:c[6].id, unit:'Tablet', purchase:22, sale:38, gst:5, stock:180, minStock:50, sku:'CF2200', expiry:mo(20), brand:'Cadila', desc:'Amlodipine Besylate 5mg', hsn:'30049099' },
    { id:uid(), name:'Vitamin C 500mg', category:c[4].id, unit:'Tablet', purchase:14, sale:28, gst:5, stock:500, minStock:50, sku:'HM8800', expiry:mo(30), brand:'Himalaya', desc:'Ascorbic Acid 500mg', hsn:'29362700' },
    { id:uid(), name:'Betadine Cream 10g', category:c[8].id, unit:'Cream', purchase:28, sale:48, gst:12, stock:60, minStock:10, sku:'WM3300', expiry:mo(22), brand:'Win Medicare', desc:'Povidone-Iodine 5%', hsn:'30049039' },
    { id:uid(), name:'Eye Drops 5ml', category:c[0].id, unit:'Drops', purchase:55, sale:90, gst:12, stock:12, minStock:10, sku:'AL2023', expiry:mo(-2), brand:'Alcon', desc:'Moxifloxacin 0.5%', hsn:'30049039' },
    { id:uid(), name:'Pantoprazole 40mg', category:c[2].id, unit:'Tablet', purchase:25, sale:45, gst:12, stock:0, minStock:30, sku:'SR4411', expiry:mo(14), brand:'Serum', desc:'Pantoprazole Sodium 40mg', hsn:'30049099' },
  ];

  // Demo bills with doctor field
  const patients = ['Ramesh Kumar','Priya Sharma','Anil Patel','Sunita Rao','Vijay Singh','Meena Joshi','Deepak Nair'];
  const doctors = ['Dr. Mehta','Dr. Singh','Dr. Verma','Dr. Pillai','Dr. Khan'];
  const modes = ['Cash','UPI','Cash','Card','UPI','Cash','Insurance','Cash'];
  const prods = STATE.products;

  for (let i = 0; i < 14; i++) {
    const billDate = daysAgo(i % 9);
    const p1 = prods[i % prods.length];
    const p2 = prods[(i + 3) % prods.length];
    const items = [makeItem(p1,(i%5)+2,0), makeItem(p2,(i%3)+1,i%3===0?5:0)];
    const totals = calcTotals(items);
    STATE.bills.unshift({
      id:uid(), billNo:String(STATE.nextBillNo++).padStart(4,'0'),
      date:billDate, customer:patients[i%patients.length], phone:'',
      doctor:doctors[i%doctors.length], rx:'',
      paymentMode:modes[i%modes.length], items, ...totals, notes:'',
      billStoreType: 'retail'
    });
  }
  // Demo stock-ins
  for (let i = 0; i < 6; i++) {
    const p = prods[i * 2 % prods.length];
    STATE.stockIns.push({ id:uid(), date:daysAgo(i+1), productId:p.id, productName:p.name, qty:50+i*10, price:p.purchase, batch:'B'+(1000+i), expiry:mo(12+i), supplier:'Main Distributor', invoiceNo:'INV-'+(100+i), notes:'' });
  }
  // Demo credit / pending payments (Wholesale)
  const creditShops = [
    { shop:'Ramesh Medical Store',   name:'Ramesh Kumar',   phone:'9876543210' },
    { shop:'Priya Pharma Traders',   name:'Priya Sharma',   phone:'9823456780' },
    { shop:'Anil Drug House',        name:'Anil Patel',     phone:'9912345678' },
    { shop:'Sunita Medicals',        name:'Sunita Rao',     phone:'9988776655' },
    { shop:'Vijay Health Store',     name:'Vijay Singh',    phone:'9765432109' },
    { shop:'Meena Pharmaceuticals',  name:'Meena Joshi',    phone:'9654321098' },
    { shop:'Deepak Medical Agency',  name:'Deepak Nair',    phone:'9543210987' },
    { shop:'Kumar Drug Centre',      name:'Suresh Kumar',   phone:'9432109876' },
    { shop:'Patel Pharma Dist.',     name:'Rakesh Patel',   phone:'9321098765' },
    { shop:'Singh Medicals',         name:'Harpreet Singh', phone:'9210987654' },
    { shop:'Jain Medical Traders',   name:'Abhay Jain',     phone:'9109876543' },
    { shop:'Sharma Drug House',      name:'Mohan Sharma',   phone:'9098765432' },
  ];
  const creditItems  = ['Paracetamol 500mg x100','Amoxicillin 250mg x50','Azithromycin 500mg x30','Cetirizine 10mg x200','Omeprazole 20mg x80','Metformin 500mg x150','Vitamin C 500mg x100','Cough Syrup 100ml x20','Amlodipine 5mg x120','Betadine Cream 10g x40','Eye Drops 5ml x60','Pantoprazole 40mg x90'];
  const creditMethods= ['UPI','NEFT','Cash','Credit/Debit Card','UPI','NEFT','Cash','UPI','NEFT','Cash','Credit/Debit Card','UPI'];
  const creditStatus = ['Pending','Cleared','Pending','Pending','Cleared','Pending','Cleared','Pending','Pending','Cleared','Pending','Cleared'];
  const creditAmounts= [1850,3200,2750,4100,1500,5600,2300,3800,2100,4900,1700,3500];
  const daysBackList = [2,5,8,12,18,22,28,35,45,60,75,85];
  STATE.credits = creditShops.map((s,i)=>({ id:uid(), date:daysAgo(daysBackList[i]), shopName:s.shop, shopkeeperName:s.name, phone:s.phone, forItem:creditItems[i], amount:creditAmounts[i], method:creditMethods[i], status:creditStatus[i] }));

  // Demo shopCredits — for Retail/Hospital/Medical/Ayurvedic
  const suppliers = [
    { supplierName:'Apex Pharma Dist.',    wId:'WHL-001', ownerName:'Rajesh Gupta'    },
    { supplierName:'MedLine Wholesale',    wId:'WHL-002', ownerName:'Sanjay Mehta'    },
    { supplierName:'BharatMed Traders',    wId:'WHL-003', ownerName:'Vikram Shah'      },
    { supplierName:'Sunrise Drug House',   wId:'WHL-004', ownerName:'Pooja Reddy'     },
    { supplierName:'National Pharma Co.',  wId:'WHL-005', ownerName:'Arvind Kumar'    },
    { supplierName:'HealthFirst Dist.',    wId:'WHL-006', ownerName:'Suresh Nair'     },
    { supplierName:'Prime Med Supply',     wId:'WHL-007', ownerName:'Deepa Iyer'      },
    { supplierName:'City Drug Traders',    wId:'WHL-008', ownerName:'Ravi Sharma'     },
    { supplierName:'Lifeline Wholesale',   wId:'WHL-009', ownerName:'Anita Patel'     },
    { supplierName:'GreenMed Dist.',       wId:'WHL-010', ownerName:'Kartik Joshi'    },
  ];
  const sPurchase=[12500,8200,15000,6800,22000,9400,11200,7600,18500,5300];
  const sPaid    =[10000,8200, 8000,6800,15000,5000, 7000,5000,12000,3000];
  const sMethods =['UPI','NEFT','Cash','UPI','NEFT','Cash','Credit/Debit Card','UPI','NEFT','Cash'];
  const sDays    =[3,8,12,20,25,32,40,50,62,75];
  STATE.shopCredits = suppliers.map((s,i)=>{
    const pending = +(sPurchase[i]-sPaid[i]).toFixed(2);
    return { id:uid(), supplierId:s.wId, supplierName:s.supplierName, ownerName:s.ownerName,
      totalPurchase:sPurchase[i], paid:sPaid[i], paymentMode:sMethods[i],
      pending, lastPurchaseDate:daysAgo(sDays[i]), billDate:daysAgo(sDays[i]),
      status: pending<=0 ? 'Cleared' : 'Pending' };
  });
  saveState();
}

function makeItem(p, qty, disc) {
  const lineTotal = qty * p.sale;
  const discAmt = lineTotal * disc / 100;
  const taxable = lineTotal - discAmt;
  const gstAmt = taxable * (p.gst / 100);
  return { productId:p.id, name:p.name, category:p.category, unit:p.unit, qty, unitPrice:p.sale, discount:disc, gstRate:p.gst, gstAmt:+gstAmt.toFixed(2), lineTotal:+(taxable+gstAmt).toFixed(2) };
}
function calcTotals(items) {
  const subtotal = items.reduce((s,it) => s + it.qty * it.unitPrice, 0);
  const totalDiscount = items.reduce((s,it) => s + it.qty * it.unitPrice * it.discount / 100, 0);
  const totalGst = items.reduce((s,it) => s + it.gstAmt, 0);
  const raw = subtotal - totalDiscount + totalGst;
  const grandTotal = Math.round(raw);
  return { subtotal:+subtotal.toFixed(2), totalDiscount:+totalDiscount.toFixed(2), totalGst:+totalGst.toFixed(2), roundOff:+(grandTotal-raw).toFixed(2), grandTotal };
}

// ── Branding ─────────────────────────────────────────
function applyBranding() {
  const s = STATE.settings;
  if (el('brand-name')) el('brand-name').textContent = s.storeName || 'PharmaCare';
  if (el('brand-type')) el('brand-type').textContent = (s.storeType || 'Pharmacy') + ' · Pro';
  // brand-icon holds the logo image — do not overwrite it
  document.title = (s.storeName || 'PharmaCare') + ' Pro — Inventory';
}

// ── Category helpers ──────────────────────────────────
function getCatName(catId) { return STATE.categories.find(c => c.id === catId)?.name || 'Uncategorized'; }
function populateCategorySelects() {
  ['pm-cat','prod-cat-filter'].forEach(selId => {
    const sel = document.getElementById(selId); if (!sel) return;
    const old = sel.value;
    const isFilter = selId === 'prod-cat-filter';
    sel.innerHTML = isFilter ? '<option value="">All Categories</option>' : '';
    STATE.categories.forEach(c => { const o = document.createElement('option'); o.value = c.id; o.textContent = c.name; sel.appendChild(o); });
    sel.value = old;
  });
}

// ══════════════════════════════════════════════════
// DASHBOARD
// ══════════════════════════════════════════════════
function getDashBills() {
  const isWholesale = (STATE.settings.storeType || '').trim() === 'Wholesale Pharma';
  const typeKey = isWholesale ? 'wholesale' : 'retail';
  const resets  = STATE.dashboardResets || {};
  const resetDate = resets[typeKey] || null;
  return STATE.bills.filter(b => {
    const bType = (b.billStoreType || 'retail');
    const typeOk = isWholesale ? bType === 'wholesale' : bType !== 'wholesale';
    if (!typeOk) return false;
    if (resetDate && b.date < resetDate) return false;
    return true;
  });
}

function getDashboardLabel() {
  const t = (STATE.settings.storeType || 'Retail Pharmacy').trim();
  if (t === 'Wholesale Pharma') return { label: 'Wholesale', color: '#0ea5e9', icon: '🏭' };
  if (t === 'Hospital Pharmacy') return { label: 'Hospital', color: '#8b5cf6', icon: '🏥' };
  if (t === 'Medical Store') return { label: 'Medical Store', color: '#f59e0b', icon: '🏪' };
  if (t === 'Ayurvedic Store') return { label: 'Ayurvedic', color: '#10b981', icon: '🌿' };
  return { label: 'Retail', color: '#0ea5e9', icon: '💊' };
}

function renderDashboard() {
  const isWholesale = (STATE.settings.storeType || '').trim() === 'Wholesale Pharma';
  const dashMeta    = getDashboardLabel();
  const resets      = STATE.dashboardResets || {};
  const typeKey     = isWholesale ? 'wholesale' : 'retail';
  const resetDate   = resets[typeKey] || null;

  // Badge showing current mode
  const dashBanner = document.getElementById('dash-type-banner');
  if (dashBanner) {
    dashBanner.innerHTML = `
      <span style="display:inline-flex;align-items:center;gap:6px;background:${dashMeta.color}18;border:1.5px solid ${dashMeta.color}40;color:${dashMeta.color};border-radius:30px;padding:4px 14px;font-size:12px;font-weight:700;letter-spacing:.3px">
        ${dashMeta.icon} ${dashMeta.label} Dashboard
        ${resetDate ? `<span style="font-weight:400;opacity:.75;margin-left:4px">· since ${fmtDate(resetDate)}</span>` : ''}
      </span>`;
  }

  const filteredBills = getDashBills();
  const todayStr  = today();
  const todayBills = filteredBills.filter(b => b.date === todayStr);
  const todayRev  = todayBills.reduce((s,b) => s + b.grandTotal, 0);
  const lowStock  = STATE.products.filter(p => p.stock <= p.minStock);
  const alertDays = STATE.settings.expiryAlertDays || 90;
  const expiring  = STATE.products.filter(p => { const d = expiryDaysLeft(p.expiry); return d >= 0 && d <= alertDays; });
  const expired   = STATE.products.filter(p => expiryDaysLeft(p.expiry) < 0);

  const statsEl = document.getElementById('dash-stats');
  if (statsEl) statsEl.innerHTML = `
    <div class="stat-card" style="--stat-color:#0ea5e9;--stat-color2:#38bdf8">
      <div class="stat-icon">💊</div>
      <div class="stat-value">${STATE.products.length}</div>
      <div class="stat-label">Total Medicines</div>
    </div>
    <div class="stat-card" style="--stat-color:#10b981;--stat-color2:#34d399">
      <div class="stat-icon">₹</div>
      <div class="stat-value">${cur(todayRev)}</div>
      <div class="stat-label">Today's Revenue</div>
      <div class="stat-trend">${todayBills.length} bills today</div>
    </div>
    <div class="stat-card" style="--stat-color:#f59e0b;--stat-color2:#fbbf24">
      <div class="stat-icon">⚠️</div>
      <div class="stat-value">${lowStock.length}</div>
      <div class="stat-label">Low Stock Items</div>
    </div>
    <div class="stat-card" style="--stat-color:#ef4444;--stat-color2:#f87171">
      <div class="stat-icon">📅</div>
      <div class="stat-value">${expired.length + expiring.length}</div>
      <div class="stat-label">Expiry Alerts</div>
      <div class="stat-trend down">${expired.length} expired</div>
    </div>
  `;

  // Low stock
  const lsEl = document.getElementById('dash-low-stock');
  if (lsEl) {
    if (lowStock.length === 0) { lsEl.innerHTML = '<p style="text-align:center;color:#94a3b8;padding:24px;font-style:italic">✓ All medicines adequately stocked</p>'; }
    else lsEl.innerHTML = lowStock.map(p => `
      <div class="low-stock-item">
        <div><div class="ls-name">${p.name}</div><div style="font-size:11px;color:#94a3b8">${getCatName(p.category)} · ${p.unit}</div></div>
        <div style="text-align:right"><div class="ls-stock">${p.stock}</div><div style="font-size:10px;color:#94a3b8">min:${p.minStock}</div></div>
      </div>`).join('');
  }

  // Expiry soon
  const exEl = document.getElementById('dash-expiry-list');
  if (exEl) {
    const soon = [...expired, ...expiring].slice(0,6);
    if (soon.length === 0) { exEl.innerHTML = '<p style="text-align:center;color:#94a3b8;padding:20px;font-style:italic">✓ No expiry alerts</p>'; }
    else exEl.innerHTML = soon.map(p => {
      const eb = getExpiryBadge(p.expiry);
      return `<div style="display:flex;justify-content:space-between;align-items:center;padding:9px 4px;border-bottom:1px solid #f1f5f9">
        <div><div style="font-size:13px;font-weight:600">${p.name}</div><div style="font-size:11px;color:#94a3b8">${p.batch||p.sku||''} · ${p.stock} units</div></div>
        <span class="badge ${eb.cls}">${eb.label}</span>
      </div>`;
    }).join('');
  }

  // Recent bills
  const rbEl = document.getElementById('dash-recent-bills');
  if (rbEl) {
    if (filteredBills.length === 0) { rbEl.innerHTML = '<p style="text-align:center;color:#94a3b8;padding:24px;font-style:italic">No bills yet for this pharmacy type</p>'; }
    else rbEl.innerHTML = filteredBills.slice(0,8).map(b => `
      <div style="display:flex;justify-content:space-between;align-items:center;padding:9px 4px;border-bottom:1px solid #f1f5f9">
        <div style="display:flex;align-items:center;gap:10px">
          <span style="font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:700;background:#f0f9ff;color:#0369a1;padding:2px 8px;border-radius:20px">#${b.billNo}</span>
          <div><div style="font-size:13px;font-weight:600">${b.customer}</div><div style="font-size:11px;color:#94a3b8">${fmtDate(b.date)} · ${b.doctor||''} · ${b.paymentMode}</div></div>
        </div>
        <div style="font-family:'JetBrains Mono',monospace;font-weight:700;color:#10b981">${cur(b.grandTotal)}</div>
      </div>`).join('');
  }

  drawRevenueChart7(filteredBills);
  drawTopProductsChart(filteredBills);
  drawProfitChart(filteredBills);
}

function drawRevenueChart7(bills) {
  destroyChart('dashRev');
  const ctx = document.getElementById('chart-revenue'); if (!ctx) return;
  const src = bills || getDashBills();
  const labels=[], data=[];
  for (let i=6; i>=0; i--) {
    const d = daysAgo(i);
    labels.push(new Date(d).toLocaleDateString('en-IN',{weekday:'short',day:'numeric'}));
    data.push(src.filter(b=>b.date===d).reduce((s,b)=>s+b.grandTotal,0));
  }
  chartInstances['dashRev'] = new Chart(ctx, { type:'bar', data:{ labels, datasets:[{ label:'Revenue', data, backgroundColor:'rgba(14,165,233,0.85)', borderRadius:7, borderSkipped:false }] }, options:{ responsive:true, plugins:{legend:{display:false}}, scales:{ y:{beginAtZero:true,grid:{color:'#f1f5f9'},ticks:{callback:v=>STATE.settings.currency+v}}, x:{grid:{display:false}} } } });
}
function drawTopProductsChart(bills) {
  destroyChart('dashTop');
  const ctx = document.getElementById('chart-top-products'); if (!ctx) return;
  const src = bills || getDashBills();
  const sales={};
  src.forEach(b=>b.items.forEach(it=>{ sales[it.name]=(sales[it.name]||0)+it.qty; }));
  const sorted = Object.entries(sales).sort((a,b)=>b[1]-a[1]).slice(0,8);
  const COLORS=['#0ea5e9','#10b981','#f97316','#8b5cf6','#f59e0b','#ec4899','#14b8a6','#ef4444'];
  chartInstances['dashTop'] = new Chart(ctx, { type:'bar', data:{ labels:sorted.map(([k])=>k.length>20?k.slice(0,20)+'…':k), datasets:[{label:'Units',data:sorted.map(([,v])=>v),backgroundColor:COLORS,borderRadius:6,borderSkipped:false}] }, options:{ indexAxis:'y', responsive:true, plugins:{legend:{display:false}}, scales:{ x:{beginAtZero:true,grid:{color:'#f1f5f9'}}, y:{grid:{display:false}} } } });
}

function drawProfitChart(bills) {
  destroyChart('dashProfit');
  const ctx = document.getElementById('chart-profit'); if (!ctx) return;
  const src = bills || getDashBills();

  // Build product lookup: productId -> purchase price
  const purchaseMap = {};
  STATE.products.forEach(p => { purchaseMap[p.id] = p.purchase || 0; });

  // Current month boundaries
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth(); // 0-indexed
  const monthStart = new Date(year, month, 1);
  const monthEnd   = new Date(year, month + 1, 0); // last day of month

  // Label for display
  const monthName = now.toLocaleDateString('en-IN', { month: 'long', year: 'numeric' });
  const labelEl = document.getElementById('profit-month-label');
  if (labelEl) labelEl.textContent = monthName;

  // Week buckets: Week 1 = days 1-7, Week 2 = 8-14, Week 3 = 15-21, Week 4 = 22-end
  const weekProfit = [0, 0, 0, 0];

  src.forEach(b => {
    const billDate = new Date(b.date);
    // Only bills in current month
    if (billDate < monthStart || billDate > monthEnd) return;
    const day = billDate.getDate();
    const weekIdx = day <= 7 ? 0 : day <= 14 ? 1 : day <= 21 ? 2 : 3;

    b.items.forEach(it => {
      const purchasePrice = purchaseMap[it.productId] || 0;
      // Profit per unit = MRP(unitPrice) - Purchase Price
      const profitPerUnit = it.unitPrice - purchasePrice;
      weekProfit[weekIdx] += profitPerUnit * it.qty;
    });
  });

  // Round to 2 decimal places
  const data = weekProfit.map(v => +v.toFixed(2));
  const labels = ['Week 1\n(1–7)', 'Week 2\n(8–14)', 'Week 3\n(15–21)', 'Week 4\n(22–end)'];
  const barColors = data.map(v => v >= 0 ? 'rgba(16,185,129,0.85)' : 'rgba(239,68,68,0.85)');

  destroyChart('dashProfit');
  chartInstances['dashProfit'] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: ['Week 1  (Days 1–7)', 'Week 2  (Days 8–14)', 'Week 3  (Days 15–21)', 'Week 4  (Days 22–end)'],
      datasets: [{
        label: 'Profit (₹)',
        data,
        backgroundColor: barColors,
        borderRadius: 8,
        borderSkipped: false
      }]
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => ' Profit: ' + (STATE.settings.currency || '₹') + ctx.parsed.y.toFixed(2)
          }
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          grid: { color: '#f1f5f9' },
          ticks: { callback: v => (STATE.settings.currency || '₹') + v }
        },
        x: { grid: { display: false } }
      }
    }
  });

  // Summary badges below chart
  const summaryEl = document.getElementById('profit-week-summary');
  if (summaryEl) {
    const weekNames = ['Week 1', 'Week 2', 'Week 3', 'Week 4'];
    summaryEl.innerHTML = data.map((v, i) => {
      const color = v >= 0 ? '#10b981' : '#ef4444';
      const bg    = v >= 0 ? '#f0fdf4' : '#fef2f2';
      return `<div style="flex:1;min-width:120px;background:${bg};border-radius:10px;padding:10px 14px;border:1px solid ${v>=0?'#bbf7d0':'#fecaca'}">
        <div style="font-size:11px;color:#94a3b8;font-weight:600">${weekNames[i]}</div>
        <div style="font-size:16px;font-weight:700;color:${color};font-family:'JetBrains Mono',monospace">${(STATE.settings.currency||'₹')}${v.toFixed(2)}</div>
      </div>`;
    }).join('');
  }
}

// ══════════════════════════════════════════════════
// DASHBOARD RESET
// ══════════════════════════════════════════════════
function resetDashboard() {
  const storeType = (STATE.settings.storeType || 'Retail Pharmacy').trim();
  const isWholesale = storeType === 'Wholesale Pharma';
  const typeKey = isWholesale ? 'wholesale' : 'retail';
  const label = isWholesale ? 'Wholesale' : 'Retail / Hospital / Medical / Ayurvedic';

  if (!confirm(
    `Reset the ${label} Dashboard?\n\n` +
    `• Stats, charts, and recent bills will only show data from today onwards.\n` +
    `• Old bills are NOT deleted — only hidden from this dashboard view.\n` +
    `• You can undo this by contacting the developer or clearing the reset date in DB.\n\n` +
    `Proceed?`
  )) return;

  const resetDate = today();
  if (!STATE.dashboardResets) STATE.dashboardResets = {};
  STATE.dashboardResets[typeKey] = resetDate;
  saveState();

  // Persist to backend
  const USE_API = typeof API_BASE !== 'undefined';
  if (USE_API) {
    fetch(`${API_BASE}/api/dashboard/reset`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ storeTypeKey: typeKey, resetDate })
    }).catch(e => console.warn('Dashboard reset sync failed:', e));
  }

  toast(`${label} Dashboard reset ✓ — showing data from today onwards`, 'ok');
  renderDashboard();
}

async function loadDashboardResets() {
  const USE_API = typeof API_BASE !== 'undefined';
  if (!USE_API) return;
  try {
    const r = await fetch(`${API_BASE}/api/dashboard/resets`);
    if (!r.ok) return;
    const data = await r.json();
    if (!STATE.dashboardResets) STATE.dashboardResets = {};
    (data.resets || []).forEach(row => {
      STATE.dashboardResets[row.storeTypeKey] = row.resetDate;
    });
  } catch(e) { console.warn('Could not load dashboard resets:', e); }
}


function renderProducts() {
  populateCategorySelects();
  const q = (document.getElementById('prod-search')?.value||'').toLowerCase();
  const cat = document.getElementById('prod-cat-filter')?.value||'';
  const status = document.getElementById('prod-status-filter')?.value||'';
  const tbody = document.getElementById('products-tbody');
  const mobileEl = document.getElementById('products-mobile');

  let prods = STATE.products.filter(p => {
    const mq = !q || p.name.toLowerCase().includes(q) || (p.sku||'').toLowerCase().includes(q) || (p.brand||'').toLowerCase().includes(q) || (p.desc||'').toLowerCase().includes(q);
    const mc = !cat || p.category===cat;
    const days = expiryDaysLeft(p.expiry);
    const ms = !status
      || (status==='low' && p.stock<=p.minStock && p.stock>0)
      || (status==='out' && p.stock===0)
      || (status==='expiring' && days>=0 && days<=90)
      || (status==='expired' && days<0)
      || (status==='ok' && p.stock>p.minStock && days>90);
    return mq && mc && ms;
  });

  if (prods.length === 0) {
    if (tbody) tbody.innerHTML='<tr class="empty-row"><td colspan="12">No medicines found</td></tr>';
    if (mobileEl) mobileEl.innerHTML='<div style="text-align:center;padding:28px;color:#94a3b8;font-style:italic">No medicines found</div>';
    return;
  }

  const margin = p => p.purchase>0 ? (((p.sale-p.purchase)/p.purchase)*100).toFixed(1) : '0';

  // Desktop table
  if (tbody) tbody.innerHTML = prods.map((p,i) => {
    let sb; const days = expiryDaysLeft(p.expiry);
    if (p.stock===0) sb='<span class="badge badge-red">Out of Stock</span>';
    else if (days<0) sb='<span class="badge badge-red">Expired</span>';
    else if (p.stock<=p.minStock && days<=30) sb='<span class="badge badge-red">Critical</span>';
    else if (p.stock<=p.minStock) sb='<span class="badge badge-amber">Low Stock</span>';
    else if (days<=30) sb='<span class="badge badge-amber">Expiring</span>';
    else sb='<span class="badge badge-green">OK</span>';
    const eb = getExpiryBadge(p.expiry);
    return `<tr>
      <td style="color:#94a3b8;font-size:12px">${i+1}</td>
      <td><div style="font-weight:600">${p.name}</div><div style="font-size:11px;color:#94a3b8">${p.brand||''} · ${p.desc||''}</div></td>
      <td><span class="badge badge-blue">${getCatName(p.category)}</span></td>
      <td style="font-size:12px;color:#64748b">${p.unit}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-size:11px">${p.sku||'—'}</td>
      <td><span class="badge ${eb.cls}" style="font-family:'JetBrains Mono',monospace;font-size:10px">${eb.label}</span></td>
      <td style="font-family:'JetBrains Mono',monospace;font-weight:600">${cur(p.sale)}</td>
      <td style="font-family:'JetBrains Mono',monospace">${cur(p.purchase)}<br><span style="font-size:10px;color:#10b981">+${margin(p)}%</span></td>
      <td>${p.gst}%</td>
      <td style="font-family:'JetBrains Mono',monospace;font-weight:700;color:${p.stock===0?'#ef4444':p.stock<=p.minStock?'#f59e0b':'#10b981'}">${p.stock}</td>
      <td>${sb}</td>
      <td style="white-space:nowrap">
        <button class="btn-icon" onclick="editProduct('${p.id}')" title="Edit">✏️</button>
        <button class="btn-icon" onclick="quickStockEdit('${p.id}')" title="Adjust Stock">📦</button>
        <button class="btn-icon" onclick="deleteProduct('${p.id}')" title="Delete">🗑️</button>
      </td>
    </tr>`;
  }).join('');

  // Mobile cards
  if (mobileEl) mobileEl.innerHTML = prods.map(p => {
    const days = expiryDaysLeft(p.expiry);
    let badgeCls, badgeText;
    if (p.stock===0) { badgeCls='badge-red'; badgeText='Out of Stock'; }
    else if (days<0) { badgeCls='badge-red'; badgeText='Expired'; }
    else if (p.stock<=p.minStock && days<=30) { badgeCls='badge-red'; badgeText='Critical'; }
    else if (p.stock<=p.minStock) { badgeCls='badge-amber'; badgeText='Low Stock'; }
    else if (days<=30) { badgeCls='badge-amber'; badgeText='Expiring'; }
    else { badgeCls='badge-green'; badgeText='OK'; }
    const eb = getExpiryBadge(p.expiry);
    return `<div class="m-card">
      <div class="m-card-hd">
        <div class="m-card-name">${p.name}</div>
        <span class="badge ${badgeCls}">${badgeText}</span>
      </div>
      <div class="m-card-row"><span>Category</span><strong>${getCatName(p.category)}</strong></div>
      <div class="m-card-row"><span>Form</span><strong>${p.unit}</strong></div>
      <div class="m-card-row"><span>MRP</span><strong style="color:var(--accent);font-family:'JetBrains Mono',monospace">${cur(p.sale)}</strong></div>
      <div class="m-card-row"><span>Purchase</span><strong style="font-family:'JetBrains Mono',monospace">${cur(p.purchase)}</strong></div>
      <div class="m-card-row"><span>Stock</span><strong style="font-family:'JetBrains Mono',monospace;color:${p.stock===0?'#ef4444':p.stock<=p.minStock?'#f59e0b':'#10b981'}">${p.stock} ${p.unit}${p.stock!==1?'s':''}</strong></div>
      <div class="m-card-row"><span>Expiry</span><span class="badge ${eb.cls}">${eb.label}</span></div>
      ${p.brand ? `<div class="m-card-row"><span>Manufacturer</span><strong>${p.brand}</strong></div>` : ''}
      ${p.sku ? `<div class="m-card-row"><span>Batch</span><strong style="font-family:'JetBrains Mono',monospace">${p.sku}</strong></div>` : ''}
      <div class="m-card-actions">
        <button class="act-edit" onclick="editProduct('${p.id}')">✏️ Edit</button>
        <button class="act-stock" onclick="quickStockEdit('${p.id}')">📦 Stock</button>
        <button class="act-del" onclick="deleteProduct('${p.id}')">🗑 Delete</button>
      </div>
    </div>`;
  }).join('');
}

// Medicine Modal
function openProductModal() {
  document.getElementById('prod-modal-title').textContent = 'Add New Medicine';
  document.getElementById('pm-edit-id').value = '';
  ['pm-name','pm-purchase','pm-sale','pm-sku','pm-brand','pm-desc','pm-expiry','pm-hsn'].forEach(id => { const el=document.getElementById(id); if(el) el.value=''; });
  document.getElementById('pm-unit').value = 'Tablet';
  document.getElementById('pm-gst').value = STATE.settings.defaultGst || 12;
  document.getElementById('pm-stock').value = 0;
  document.getElementById('pm-min-stock').value = STATE.settings.lowStockThreshold || 10;
  populateCategorySelects();
  openModal('product-modal');
}

function editProduct(id) {
  const p = STATE.products.find(x=>x.id===id); if (!p) return;
  populateCategorySelects();
  document.getElementById('prod-modal-title').textContent = 'Edit Medicine';
  document.getElementById('pm-edit-id').value = id;
  document.getElementById('pm-name').value = p.name;
  document.getElementById('pm-cat').value = p.category;
  document.getElementById('pm-unit').value = p.unit;
  document.getElementById('pm-purchase').value = p.purchase;
  document.getElementById('pm-sale').value = p.sale;
  document.getElementById('pm-gst').value = p.gst;
  document.getElementById('pm-stock').value = p.stock;
  document.getElementById('pm-min-stock').value = p.minStock;
  document.getElementById('pm-sku').value = p.sku||'';
  document.getElementById('pm-expiry').value = p.expiry||'';
  document.getElementById('pm-brand').value = p.brand||'';
  document.getElementById('pm-hsn').value = p.hsn||'';
  document.getElementById('pm-desc').value = p.desc||'';
  openModal('product-modal');
}

function saveProduct() {
  const name = document.getElementById('pm-name').value.trim();
  const cat = document.getElementById('pm-cat').value;
  const purchase = parseFloat(document.getElementById('pm-purchase').value);
  const sale = parseFloat(document.getElementById('pm-sale').value);
  if (!name||!cat||isNaN(purchase)||isNaN(sale)) { toast('Please fill all required fields','err'); return; }
  const data = {
    name, category:cat, unit:document.getElementById('pm-unit').value,
    purchase, sale, gst:parseFloat(document.getElementById('pm-gst').value)||0,
    stock:parseInt(document.getElementById('pm-stock').value)||0,
    minStock:parseInt(document.getElementById('pm-min-stock').value)||10,
    sku:document.getElementById('pm-sku').value.trim(),
    expiry:document.getElementById('pm-expiry').value,
    brand:document.getElementById('pm-brand').value.trim(),
    hsn:document.getElementById('pm-hsn').value.trim(),
    desc:document.getElementById('pm-desc').value.trim()
  };
  const editId = document.getElementById('pm-edit-id').value;
  if (editId) { const idx=STATE.products.findIndex(p=>p.id===editId); STATE.products[idx]={...STATE.products[idx],...data}; toast('Medicine updated ✓'); }
  else { STATE.products.push({id:uid(),...data}); toast('Medicine added ✓'); }
  saveState(); closeModal('product-modal'); renderProducts(); renderCategories();
}

function deleteProduct(id) {
  if (!confirm('Delete this medicine? This cannot be undone.')) return;
  STATE.products = STATE.products.filter(p=>p.id!==id);
  saveState(); renderProducts(); toast('Medicine deleted');
}

// ── Stock Adjustment Modal ────────────────────────────
let _sadjProductId = null;
let _sadjMode = 'add'; // 'add' | 'remove' | 'set'

function quickStockEdit(id) {
  const p = STATE.products.find(x => x.id === id); if (!p) return;
  _sadjProductId = id;
  _sadjMode = 'add';
  document.getElementById('sadj-name').textContent = p.name;
  document.getElementById('sadj-current').textContent = p.stock;
  document.getElementById('sadj-unit').textContent = p.unit;
  document.getElementById('sadj-min').textContent = p.minStock || '—';
  document.getElementById('sadj-qty').value = '';
  document.getElementById('sadj-preview').style.display = 'none';
  setSadjMode('add');
  openModal('stock-adj-modal');
  setTimeout(() => document.getElementById('sadj-qty').focus(), 120);
}

function setSadjMode(mode) {
  _sadjMode = mode;
  const labels = { add: 'Quantity to Add', remove: 'Quantity to Remove', set: 'Set Exact Stock To' };
  document.getElementById('sadj-qty-label').textContent = labels[mode];

  const btnAdd    = document.getElementById('sadj-btn-add');
  const btnRemove = document.getElementById('sadj-btn-remove');
  const btnSet    = document.getElementById('sadj-btn-set');

  [btnAdd, btnRemove, btnSet].forEach(b => b.classList.replace('btn-primary','btn-outline'));
  const active = mode === 'add' ? btnAdd : mode === 'remove' ? btnRemove : btnSet;
  active.classList.replace('btn-outline','btn-primary');
  updateSadjPreview();
}

function updateSadjPreview() {
  const p = STATE.products.find(x => x.id === _sadjProductId); if (!p) return;
  const qty = parseInt(document.getElementById('sadj-qty').value);
  const preview = document.getElementById('sadj-preview');
  const newValEl = document.getElementById('sadj-new-val');
  if (isNaN(qty) || qty < 0) { preview.style.display = 'none'; return; }
  let newStock;
  if (_sadjMode === 'add')    newStock = p.stock + qty;
  else if (_sadjMode === 'remove') newStock = p.stock - qty;
  else newStock = qty;
  preview.style.display = 'block';
  newValEl.textContent = Math.max(0, newStock) + ' ' + p.unit + 's';
  newValEl.style.color = newStock < 0 ? '#ef4444' : '#10b981';
}

function closeStockAdjModal() {
  closeModal('stock-adj-modal');
  _sadjProductId = null;
}

function confirmStockAdj() {
  const p = STATE.products.find(x => x.id === _sadjProductId); if (!p) return;
  const qty = parseInt(document.getElementById('sadj-qty').value);
  if (isNaN(qty) || qty < 0) { toast('Please enter a valid quantity', 'err'); return; }
  let newStock;
  if (_sadjMode === 'add')    newStock = p.stock + qty;
  else if (_sadjMode === 'remove') newStock = p.stock - qty;
  else newStock = qty;
  if (newStock < 0) { toast('Stock cannot go below 0 — reduce the quantity', 'err'); return; }
  const idx = STATE.products.findIndex(x => x.id === _sadjProductId);
  STATE.products[idx].stock = newStock;
  saveState();
  renderProducts();
  // Always refresh Stock Details low-stock table (it checks for elements internally)
  renderStockInHistory();
  // Refresh dashboard low-stock if visible
  if (document.getElementById('page-dashboard')?.classList.contains('active')) {
    renderDashboard();
  }
  closeStockAdjModal();
  toast(`✓ Stock updated to ${newStock} ${p.unit}s`);
}

// ══════════════════════════════════════════════════
// STOCK IN
// ══════════════════════════════════════════════════
function showStockInDropdown() {
  const q = (document.getElementById('si-prod-search')?.value||'').toLowerCase();
  const dd = document.getElementById('si-dropdown');
  if (!q||!dd) { dd?.classList.add('hidden'); return; }
  const matches = STATE.products.filter(p=>p.name.toLowerCase().includes(q));
  if (matches.length===0) { dd.innerHTML='<div class="dd-item"><span class="dd-meta">No medicines found</span></div>'; dd.classList.remove('hidden'); return; }
  dd.innerHTML = matches.slice(0,8).map(p=>`
    <div class="dd-item" onclick="selectStockInProduct('${p.id}')">
      <div class="dd-name">${p.name}</div>
      <div class="dd-meta">${getCatName(p.category)} · ${p.unit} · Stock: ${p.stock} · Batch: ${p.sku||'—'}</div>
    </div>`).join('');
  dd.classList.remove('hidden');
}

function selectStockInProduct(id) {
  const p = STATE.products.find(x=>x.id===id); if (!p) return;
  selectedStockInProduct = p;
  document.getElementById('si-prod-search').value = p.name;
  document.getElementById('si-dropdown').classList.add('hidden');
  document.getElementById('si-price').value = p.purchase;
  document.getElementById('si-batch').value = p.sku||'';
  document.getElementById('si-expiry').value = p.expiry||'';
  const info = document.getElementById('si-selected-info');
  info.classList.remove('hidden');
  info.innerHTML = `<strong>${p.name}</strong> · ${getCatName(p.category)} · Current Stock: <strong>${p.stock}</strong> · Batch: <strong>${p.sku||'—'}</strong>`;
}

function saveStockIn() {
  if (!selectedStockInProduct) { toast('Select a medicine first','err'); return; }
  const qty = parseInt(document.getElementById('si-qty').value);
  const price = parseFloat(document.getElementById('si-price').value);
  const batch = document.getElementById('si-batch').value.trim();
  const expiry = document.getElementById('si-expiry').value;
  if (!qty||qty<1||isNaN(price)) { toast('Enter valid quantity and price','err'); return; }
  const idx = STATE.products.findIndex(p=>p.id===selectedStockInProduct.id);
  STATE.products[idx].stock += qty;
  STATE.products[idx].purchase = price;
  if (batch) STATE.products[idx].sku = batch;
  if (expiry) STATE.products[idx].expiry = expiry;
  STATE.stockIns.unshift({ id:uid(), date:today(), productId:selectedStockInProduct.id, productName:selectedStockInProduct.name, qty, price, batch, expiry, supplier:document.getElementById('si-supplier').value.trim(), invoiceNo:document.getElementById('si-invoice').value.trim(), notes:document.getElementById('si-notes').value.trim() });
  saveState();
  toast(`Added ${qty} units of ${selectedStockInProduct.name} ✓`);
  ['si-prod-search','si-qty','si-price','si-batch','si-supplier','si-invoice','si-notes'].forEach(id=>{ const el=document.getElementById(id); if(el) el.value=''; });
  document.getElementById('si-expiry').value='';
  document.getElementById('si-selected-info').classList.add('hidden');
  selectedStockInProduct = null;
  renderStockInHistory();
  renderProducts();
  renderCategories();
}

function renderStockInHistory() {
  const tbody = document.getElementById('stock-in-tbody'); if (!tbody) return;
  if (STATE.stockIns.length===0) { tbody.innerHTML='<tr class="empty-row"><td colspan="7">No entries yet</td></tr>'; }
  else tbody.innerHTML = STATE.stockIns.slice(0,50).map(s=>`
    <tr>
      <td style="font-size:12px">${fmtDate(s.date)}</td>
      <td><div style="font-weight:600">${s.productName}</div><div style="font-size:11px;color:#94a3b8">${s.invoiceNo||''}</div></td>
      <td style="font-family:'JetBrains Mono',monospace;font-weight:700;color:#0ea5e9">+${s.qty}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-size:11px">${s.batch||'—'}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-size:11px;color:${expiryDaysLeft(s.expiry)<0?'#ef4444':expiryDaysLeft(s.expiry)<=90?'#f59e0b':'#64748b'}">${fmtMonth(s.expiry)||'—'}</td>
      <td style="font-family:'JetBrains Mono',monospace">${cur(s.price)}</td>
      <td style="font-size:12px">${s.supplier||'—'}</td>
    </tr>`).join('');
  renderLowStockTable();
}

function renderLowStockTable() {
  const tbody = document.getElementById('low-stock-alert-tbody');
  const countEl = document.getElementById('low-stock-count');
  if (!tbody) return;

  // Filter medicines where current stock <= minStock (the alert threshold)
  const lowProds = STATE.products
    .filter(p => p.stock <= p.minStock)
    .sort((a, b) => {
      // Sort: Out of Stock first, then by how far below minimum
      if (a.stock === 0 && b.stock !== 0) return -1;
      if (b.stock === 0 && a.stock !== 0) return 1;
      return (a.stock / a.minStock) - (b.stock / b.minStock);
    });

  if (countEl) {
    countEl.textContent = lowProds.length > 0 ? `${lowProds.length} medicine${lowProds.length !== 1 ? 's' : ''} need restocking` : '✓ All stock levels OK';
    countEl.style.background = lowProds.length > 0 ? '#fef3c7' : '#f0fdf4';
    countEl.style.color      = lowProds.length > 0 ? '#b45309'  : '#15803d';
    countEl.style.borderColor= lowProds.length > 0 ? '#fde68a'  : '#bbf7d0';
  }

  if (lowProds.length === 0) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="9" style="text-align:center;padding:28px;color:#94a3b8;font-style:italic">&#10003; All medicines are adequately stocked</td></tr>`;
    return;
  }

  tbody.innerHTML = lowProds.map((p, i) => {
    const shortage = p.minStock - p.stock;  // how many units short of the minimum
    let statusBadge, rowBg;
    if (p.stock === 0) {
      statusBadge = '<span class="badge badge-red">Out of Stock</span>';
      rowBg = 'background:#fef2f2';
    } else if (p.stock <= Math.floor(p.minStock * 0.5)) {
      statusBadge = '<span class="badge badge-red">Critical</span>';
      rowBg = 'background:#fff7ed';
    } else {
      statusBadge = '<span class="badge badge-amber">Low Stock</span>';
      rowBg = 'background:#fffbeb';
    }

    return `<tr style="${rowBg}">
      <td style="color:#94a3b8;font-size:12px">${i + 1}</td>
      <td>
        <div style="font-weight:600">${p.name}</div>
        <div style="font-size:11px;color:#94a3b8">${p.brand || ''}</div>
      </td>
      <td><span class="badge badge-blue">${getCatName(p.category)}</span></td>
      <td style="font-size:12px;color:#64748b">${p.unit}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-size:11px">${p.sku || '—'}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-weight:700;color:${p.stock === 0 ? '#ef4444' : '#f59e0b'}">${p.stock}</td>
      <td style="font-family:'JetBrains Mono',monospace;color:#64748b">${p.minStock}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-weight:600;color:#ef4444">+${shortage} needed</td>
      <td>${statusBadge}</td>
    </tr>`;
  }).join('');
}

// ══════════════════════════════════════════════════
// BILLING
// ══════════════════════════════════════════════════
function updateBillNo() {
  const isWholesale = (STATE.settings.storeType || '').trim() === 'Wholesale Pharma';
  const num = String(STATE.nextBillNo).padStart(4, '0');
  const el = document.getElementById('bill-no-display');
  if (el) el.textContent = num;
  if (isWholesale) {
    const wsNo = document.getElementById('bill-ws-no-display');
    const gstin = STATE.settings.gstin || 'GSTIN';
    if (wsNo) wsNo.value = `${gstin}-${num}`;
  } else {
    // Retail / Hospital / Medical / Ayurvedic
    const rtNo = document.getElementById('bill-rt-no-display');
    if (rtNo) rtNo.value = `#${num}`;
  }
}

function updateBillingLayout() {
  const isWholesale = (STATE.settings.storeType || '').trim() === 'Wholesale Pharma';
  const retailFields = document.getElementById('bill-fields-retail');
  const wsFields     = document.getElementById('bill-fields-wholesale');
  if (!retailFields || !wsFields) return;

  if (isWholesale) {
    retailFields.classList.add('hidden');
    wsFields.classList.remove('hidden');
    // Populate read-only from settings
    const s = STATE.settings;
    const supEl   = document.getElementById('ws-supplier-display');
    const ownEl   = document.getElementById('ws-owner-display');
    const gstEl   = document.getElementById('ws-gstin-display');
    const hideSup = document.getElementById('bill-ws-supplier');
    const hideOwn = document.getElementById('bill-ws-owner');
    const hideGst = document.getElementById('bill-ws-gstin');
    if (supEl)   supEl.textContent   = s.supplierName  || s.storeName || '—';
    if (ownEl)   ownEl.textContent   = s.ownerName     || '—';
    if (gstEl)   gstEl.textContent   = s.gstin         || '—';
    if (hideSup) hideSup.value       = s.supplierName  || s.storeName || '';
    if (hideOwn) hideOwn.value       = s.ownerName     || '';
    if (hideGst) hideGst.value       = s.gstin         || '';
    // Reset shopkeeper-specific fields
    ['bill-shop-name','bill-ws-cust-name','bill-ws-cust-phone','bill-shopkeeper-gstin'].forEach(id => {
      const el = document.getElementById(id); if (el) el.value = '';
    });
    const wsDate = document.getElementById('bill-ws-date'); if (wsDate) wsDate.value = today();
  } else {
    // ── Retail / Hospital / Medical / Ayurvedic ──
    retailFields.classList.remove('hidden');
    wsFields.classList.add('hidden');
    // Populate auto-filled display + hidden inputs from settings
    const s = STATE.settings;
    const shopVal    = s.shopName      || s.storeName || '';
    const ownerVal   = s.retailerOwner || '';
    const phoneVal   = s.phone         || '';
    const gstinVal   = s.gstin         || '';
    const licenseVal = s.license       || '';
    const emailVal   = s.email         || '';
    // Display spans
    const rtShopDisp    = el('rt-shop-display');
    const rtOwnerDisp   = el('rt-owner-display');
    const rtPhoneDisp   = el('rt-phone-display');
    const rtGstinDisp   = el('rt-gstin-display');
    const rtLicenseDisp = el('rt-license-display');
    if (rtShopDisp)    rtShopDisp.textContent    = shopVal    || '—';
    if (rtOwnerDisp)   rtOwnerDisp.textContent   = ownerVal   || '—';
    if (rtPhoneDisp)   rtPhoneDisp.textContent   = phoneVal   || '—';
    if (rtGstinDisp)   rtGstinDisp.textContent   = gstinVal   || '—';
    if (rtLicenseDisp) rtLicenseDisp.textContent = licenseVal || '—';
    // Hidden inputs
    const rtShopH    = el('bill-rt-shop');
    const rtOwnerH   = el('bill-rt-owner');
    const rtGstinH   = el('bill-rt-gstin');
    const rtLicenseH = el('bill-rt-license');
    const rtEmailH   = el('bill-rt-email');
    const rtPhoneH   = el('bill-rt-phone');
    if (rtShopH)    rtShopH.value    = shopVal;
    if (rtOwnerH)   rtOwnerH.value   = ownerVal;
    if (rtGstinH)   rtGstinH.value   = gstinVal;
    if (rtLicenseH) rtLicenseH.value = licenseVal;
    if (rtEmailH)   rtEmailH.value   = emailVal;
    if (rtPhoneH)   rtPhoneH.value   = phoneVal;
  }
  // Reset date
  const bd = document.getElementById('bill-date'); if (bd) bd.value = today();
  updateBillNo();
  updateBillingQrPanel();
}

function showBillDropdown() {
  const q = (document.getElementById('bill-prod-search')?.value||'').toLowerCase();
  const dd = document.getElementById('bill-dropdown');
  if (!q||!dd) { dd?.classList.add('hidden'); return; }
  const matches = STATE.products.filter(p=>p.name.toLowerCase().includes(q)||(p.sku||'').toLowerCase().includes(q));
  if (matches.length===0) { dd.innerHTML='<div class="dd-item"><span class="dd-meta">No medicines found</span></div>'; dd.classList.remove('hidden'); return; }
  dd.innerHTML = matches.slice(0,8).map(p=>`
    <div class="dd-item" onclick="selectBillProduct('${p.id}')">
      <div class="dd-name">${p.name} ${p.stock<=0?'<span style="color:#ef4444;font-size:11px">[Out of Stock]</span>':expiryDaysLeft(p.expiry)<0?'<span style="color:#ef4444;font-size:11px">[Expired]</span>':''}</div>
      <div class="dd-meta">MRP: ${cur(p.sale)} · Stock: ${p.stock} · Batch: ${p.sku||'—'} · Exp: ${fmtMonth(p.expiry)||'—'}</div>
    </div>`).join('');
  dd.classList.remove('hidden');
}

function selectBillProduct(id) {
  const p = STATE.products.find(x=>x.id===id); if (!p) return;
  if (expiryDaysLeft(p.expiry) < 0) { if (!confirm(`⚠️ "${p.name}" is EXPIRED (${fmtMonth(p.expiry)}). Add anyway?`)) return; }
  selectedBillProduct = p;
  document.getElementById('bill-prod-search').value = p.name;
  document.getElementById('bill-dropdown').classList.add('hidden');
  document.getElementById('bill-unit-price').value = p.sale;
  document.getElementById('bill-qty').value = 1;
  document.getElementById('bill-disc').value = 0;
  const info = document.getElementById('bill-selected-info');
  info.classList.remove('hidden');
  const eb = getExpiryBadge(p.expiry);
  info.innerHTML = `<strong>${p.name}</strong> · ${p.unit} · MRP: <strong>${cur(p.sale)}</strong> · GST: <strong>${p.gst}%</strong> · Batch: ${p.sku||'—'} · Expiry: <span class="badge ${eb.cls}" style="font-size:10px">${eb.label}</span> · Stock: <strong style="color:${p.stock>0?'#10b981':'#ef4444'}">${p.stock}</strong>`;
}

function addBillItem() {
  if (!selectedBillProduct) { toast('Select a medicine first','err'); return; }
  const qty = parseFloat(document.getElementById('bill-qty').value)||1;
  const unitPrice = parseFloat(document.getElementById('bill-unit-price').value)||selectedBillProduct.sale;
  const disc = parseFloat(document.getElementById('bill-disc').value)||0;
  if (qty<=0) { toast('Quantity must be positive','err'); return; }
  const existing = billItems.findIndex(it=>it.productId===selectedBillProduct.id);
  const item = makeItemFromInput(selectedBillProduct, qty, unitPrice, disc);
  if (existing>=0) { billItems[existing].qty += qty; recalcItem(billItems[existing]); }
  else billItems.push(item);
  selectedBillProduct=null;
  document.getElementById('bill-prod-search').value='';
  document.getElementById('bill-qty').value=1;
  document.getElementById('bill-disc').value=0;
  document.getElementById('bill-selected-info').classList.add('hidden');
  renderBillItems();
}

function makeItemFromInput(p, qty, unitPrice, disc) {
  const lineGross = qty*unitPrice; const discAmt=lineGross*disc/100; const taxable=lineGross-discAmt; const gstAmt=taxable*(p.gst/100);
  return { productId:p.id, name:p.name, category:p.category, unit:p.unit, qty, unitPrice, discount:disc, gstRate:p.gst, gstAmt:+gstAmt.toFixed(2), lineTotal:+(taxable+gstAmt).toFixed(2) };
}
function recalcItem(item) {
  const lineGross=item.qty*item.unitPrice; const discAmt=lineGross*item.discount/100; const taxable=lineGross-discAmt;
  item.gstAmt=+(taxable*item.gstRate/100).toFixed(2); item.lineTotal=+(taxable+item.gstAmt).toFixed(2);
}

function renderBillItems() {
  const tbody=document.getElementById('bill-items-tbody'); const mobileEl=document.getElementById('bill-items-mobile');
  if (billItems.length===0) {
    if (tbody) tbody.innerHTML='<tr class="empty-row"><td colspan="8">No items added yet</td></tr>';
    if (mobileEl) mobileEl.innerHTML='<div style="text-align:center;padding:20px;color:#94a3b8;font-style:italic">No items added</div>';
    updateBillSummary(0,0,0,0,0); return;
  }
  if (tbody) tbody.innerHTML = billItems.map((it,i)=>`
    <tr>
      <td style="color:#94a3b8">${i+1}</td>
      <td><div style="font-weight:600">${it.name}</div><div style="font-size:11px;color:#94a3b8">${it.unit}</div></td>
      <td><div style="display:flex;align-items:center;gap:5px">
        <button class="btn-icon" style="color:#ef4444;font-weight:bold;font-size:16px" onclick="changeBillItemQty(${i},-1)">−</button>
        <span style="font-family:'JetBrains Mono',monospace;font-weight:600;min-width:28px;text-align:center">${it.qty}</span>
        <button class="btn-icon" style="color:#10b981;font-weight:bold;font-size:16px" onclick="changeBillItemQty(${i},1)">+</button>
      </div></td>
      <td style="font-family:'JetBrains Mono',monospace">${cur(it.unitPrice)}</td>
      <td>${it.discount}%</td>
      <td style="font-size:12px;color:#64748b">${cur(it.gstAmt)}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-weight:700;color:var(--accent)">${cur(it.lineTotal)}</td>
      <td><button class="btn-icon" onclick="removeBillItem(${i})">🗑</button></td>
    </tr>`).join('');
  if (mobileEl) mobileEl.innerHTML = billItems.map((it,i)=>`
    <div class="mobile-bill-item">
      <div class="mobile-bill-item-row"><span class="mbi-name">${it.name}</span><span class="mbi-total">${cur(it.lineTotal)}</span></div>
      <div class="mbi-meta">${it.unit} · ${cur(it.unitPrice)} each${it.discount?' · '+it.discount+'% off':''} · GST: ${cur(it.gstAmt)}</div>
      <div class="mbi-qty-ctrl">
        <button class="qty-btn minus" onclick="changeBillItemQty(${i},-1)">−</button>
        <span class="qty-val">${it.qty}</span>
        <button class="qty-btn plus" onclick="changeBillItemQty(${i},1)">+</button>
        <span style="font-size:12px;color:#94a3b8;margin-left:4px">qty</span>
        <button class="btn-icon" style="margin-left:auto;color:#ef4444" onclick="removeBillItem(${i})">🗑</button>
      </div>
    </div>`).join('');
  const t=calcTotals(billItems);
  updateBillSummary(t.subtotal,t.totalDiscount,t.totalGst,t.roundOff,t.grandTotal);
}

function changeBillItemQty(idx, delta) { billItems[idx].qty=Math.max(1,billItems[idx].qty+delta); recalcItem(billItems[idx]); renderBillItems(); }
function removeBillItem(idx) { billItems.splice(idx,1); renderBillItems(); }
function clearBillItems() { if (!billItems.length||!confirm('Clear all bill items?')) return; billItems=[]; renderBillItems(); }
function updateBillSummary(sub,disc,gst,round,total) {
  const c=v=>(STATE.settings.currency||'₹')+parseFloat(v||0).toFixed(2);
  const set=(id,val)=>{ const el=document.getElementById(id); if(el) el.textContent=val; };
  set('sum-sub',c(sub)); set('sum-disc','-'+c(disc)); set('sum-gst',c(gst)); set('sum-round',c(round)); set('sum-total',c(total));
}

function finalizeBill() {
  if (!billItems.length) { toast('Add at least one medicine to the bill','err'); return; }
  const isWholesale = (STATE.settings.storeType || '').trim() === 'Wholesale Pharma';
  const s = STATE.settings;

  let customer, phone, doctor, date, paymentMode, notes;
  let wsSupplier = '', wsOwner = '', wsGstin = '', shopName = '', shopkeeperGstin = '';
  // Retail / Hospital / Medical / Ayurvedic extras (from settings, carried via hidden inputs)
  let rtShop = '', rtOwner = '', rtGstin = '', rtLicense = '', rtEmail = '', rtPhone = '';

  if (isWholesale) {
    shopName   = document.getElementById('bill-shop-name')?.value.trim() || '';
    customer   = document.getElementById('bill-ws-cust-name')?.value.trim() || '';
    phone      = document.getElementById('bill-ws-cust-phone')?.value.trim() || '';
    shopkeeperGstin = document.getElementById('bill-shopkeeper-gstin')?.value.trim() || '';
    date        = document.getElementById('bill-ws-date')?.value || today();
    paymentMode = document.getElementById('bill-ws-payment')?.value || 'Cash';
    notes       = document.getElementById('bill-notes')?.value.trim() || '';
    wsSupplier  = document.getElementById('bill-ws-supplier')?.value || s.supplierName || s.storeName || '';
    wsOwner     = document.getElementById('bill-ws-owner')?.value || s.ownerName || '';
    wsGstin     = document.getElementById('bill-ws-gstin')?.value || s.gstin || '';
    doctor      = customer;
    if (!shopName)   { toast('Shop / Retail Name is required', 'err'); return; }
    if (!customer)   { toast('Shopkeeper / Retailer Name is required', 'err'); return; }
  } else {
    // Retail / Hospital / Medical / Ayurvedic
    customer    = document.getElementById('bill-cust-name')?.value.trim() || 'Walk-in';
    phone       = document.getElementById('bill-cust-phone')?.value.trim() || '';
    doctor      = document.getElementById('bill-doctor')?.value.trim() || '';
    date        = document.getElementById('bill-date')?.value || today();
    paymentMode = document.getElementById('bill-payment')?.value || 'Cash';
    notes       = document.getElementById('bill-notes')?.value.trim() || '';
    // Collect hidden settings values
    rtShop    = document.getElementById('bill-rt-shop')?.value    || s.shopName || s.storeName || '';
    rtOwner   = document.getElementById('bill-rt-owner')?.value   || s.retailerOwner || '';
    rtGstin   = document.getElementById('bill-rt-gstin')?.value   || s.gstin || '';
    rtLicense = document.getElementById('bill-rt-license')?.value || s.license || '';
    rtEmail   = document.getElementById('bill-rt-email')?.value   || s.email || '';
    rtPhone   = document.getElementById('bill-rt-phone')?.value   || s.phone || '';
  }

  const totals = calcTotals(billItems);
  const num = String(STATE.nextBillNo).padStart(4, '0');
  const billNo = isWholesale && wsGstin ? `${wsGstin}-${num}` : num;

  const bill = {
    id: uid(), billNo, date, customer, phone, doctor, paymentMode, notes,
    items: JSON.parse(JSON.stringify(billItems)), ...totals,
    // tag bill with store type so histories stay separate
    billStoreType: isWholesale ? 'wholesale' : 'retail',
    // wholesale extras
    ...(isWholesale ? { wsSupplier, wsOwner, wsGstin, shopName, shopkeeperGstin } : {}),
    // retail extras (from settings snapshot)
    ...(!isWholesale ? { rtShop, rtOwner, rtGstin, rtLicense, rtEmail, rtPhone } : {})
  };

  bill.items.forEach(it=>{ const idx=STATE.products.findIndex(p=>p.id===it.productId); if(idx>=0) STATE.products[idx].stock=Math.max(0,STATE.products[idx].stock-it.qty); });
  STATE.nextBillNo++;
  STATE.bills.unshift(bill); saveState();
  toast(`Bill #${bill.billNo} generated ✓`);

  // Low stock alert
  const alertItems = bill.items.map(it=>{
    const p = STATE.products.find(x=>x.id===it.productId);
    if (p && p.stock <= p.minStock) return `${p.name} — Stock: ${p.stock} (Min: ${p.minStock})`;
    return null;
  }).filter(Boolean);
  if (alertItems.length && STATE.settings.email) {
    sendAlertEmail(
      `⚠️ Low Stock Alert — ${STATE.settings.storeName}`,
      `Hello,\n\nThe following medicines have reached or fallen below minimum stock after Bill #${bill.billNo}:\n\n${alertItems.join('\n')}\n\nPlease restock at the earliest.\n\nRegards,\n${STATE.settings.storeName}`
    );
  }

  billItems = [];
  renderBillItems();
  if (isWholesale) {
    ['bill-shop-name','bill-ws-cust-name','bill-ws-cust-phone','bill-shopkeeper-gstin'].forEach(id=>{ const el=document.getElementById(id); if(el) el.value=''; });
    document.getElementById('bill-ws-date').value = today();
    updateBillingLayout();
  } else {
    ['bill-cust-name','bill-cust-phone','bill-doctor','bill-notes'].forEach(id=>{ const el=document.getElementById(id); if(el) el.value=''; });
    document.getElementById('bill-date').value = today();
  }
  updateBillNo();
  showBillView(bill);
}

function printLastBill() { if (!STATE.bills.length) { toast('No bills yet','err'); return; } showBillView(STATE.bills[0]); }

function showBillView(bill) {
  if (!bill) return;
  const s = STATE.settings;
  const type = (s.storeType || 'Retail Pharmacy').trim();
  const isWholesale = type === 'Wholesale Pharma';
  const cur2 = v => (s.currency || '₹') + parseFloat(v || 0).toFixed(2);

  const rows = bill.items.map((it,i)=>`<tr><td>${i+1}</td><td>${it.name}</td><td>${it.unit}</td><td>${it.qty}</td><td>${cur2(it.unitPrice)}</td><td>${it.discount}%</td><td>${it.gstRate}% (${cur2(it.gstAmt)})</td><td style="font-weight:700">${cur2(it.lineTotal)}</td></tr>`).join('');

  let billHTML = '';

  if (isWholesale) {
    // ── Wholesale PDF (unchanged design) ──
    const sup = bill.wsSupplier || s.supplierName || s.storeName || '';
    const own = bill.wsOwner    || s.ownerName    || '';
    const gst = bill.wsGstin   || s.gstin         || '';
    const wsParts = [];
    if (sup) wsParts.push(`Supplier: ${sup}`);
    if (own) wsParts.push(`Owner: ${own}`);
    if (gst) wsParts.push(`GSTIN (Wholesaler): ${gst}`);
    const extraHeaderLines = wsParts.length ? `<div class="print-store-info" style="color:#0ea5e9;font-weight:600">${wsParts.join(' &nbsp;|&nbsp; ')}</div>` : '';
    const wsQr = s.wholesaleUpiQr || '';
    const qrBlock = wsQr ? `<div style="flex-shrink:0;display:flex;flex-direction:column;align-items:center;gap:4px;margin-left:16px"><img src="${wsQr}" style="width:90px;height:90px;object-fit:contain;border:1.5px solid #bae6fd;border-radius:8px;background:#f8fafc"/><div style="font-size:9px;color:#64748b;font-weight:600;letter-spacing:0.5px">SCAN TO PAY</div></div>` : '';
    billHTML = `
    <div class="print-doc">
      <div class="print-header" style="display:flex;justify-content:space-between;align-items:flex-start">
        <div style="flex:1;min-width:0;text-align:center">
          <div class="print-store-name">${s.storeName||'My Pharmacy'}</div>
          <div class="print-store-info">${type}${s.address ? ' | ' + s.address : ''}</div>
          ${extraHeaderLines}
          <div class="print-store-info">${s.phone?'📞 '+s.phone:''} ${s.email?'| ✉ '+s.email:''}</div>
          <div class="print-store-info">${s.gstin?'GSTIN: '+s.gstin:''} ${s.license?'| DL No: '+s.license:''}</div>
        </div>
        ${qrBlock}
      </div>
      <div class="print-meta">
        <div><strong>Bill No:</strong> ${bill.billNo}</div>
        <div><strong>Date:</strong> ${fmtDate(bill.date)}</div>
        <div><strong>Shop / Retail Name:</strong> ${bill.shopName || '—'}</div>
        <div><strong>Shopkeeper Name:</strong> ${bill.customer || '—'}</div>
        <div><strong>Phone (Shopkeeper):</strong> ${bill.phone || '—'}</div>
        <div><strong>GSTIN (Shopkeeper):</strong> ${bill.shopkeeperGstin || '—'}</div>
        <div><strong>Payment:</strong> ${bill.paymentMode}</div>
        ${bill.notes ? `<div><strong>Notes:</strong> ${bill.notes}</div>` : ''}
      </div>
      <table class="print-items-table">
        <thead><tr><th>#</th><th>Medicine</th><th>Form</th><th>Qty</th><th>MRP</th><th>Disc</th><th>GST</th><th>Amount</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
      <div class="print-totals">
        <div>Subtotal: ${cur2(bill.subtotal)}</div>
        <div>Discount: -${cur2(bill.totalDiscount)}</div>
        <div>GST: ${cur2(bill.totalGst)}</div>
        ${bill.roundOff?`<div>Round Off: ${cur2(bill.roundOff)}</div>`:''}
        <div class="print-grand-total">GRAND TOTAL: ${cur2(bill.grandTotal)}</div>
      </div>
      <div class="print-footer">Thank you for choosing ${s.storeName||'our pharmacy'} · Get well soon! 💊<br/>Computer generated bill — no signature required · Licensed Pharmacy</div>
    </div>`;
  } else {
    // ── Retail / Hospital / Medical / Ayurvedic — Professional PDF ──
    // Prefer snapshot values stored in bill, fallback to live settings
    const rtShop    = bill.rtShop    || s.shopName      || s.storeName || '';
    const rtOwner   = bill.rtOwner   || s.retailerOwner || '';
    const rtGstin   = bill.rtGstin   || s.gstin         || '';
    const rtLicense = bill.rtLicense || s.license       || '';
    const rtEmail   = bill.rtEmail   || s.email         || '';
    const rtPhone   = bill.rtPhone   || s.phone         || '';
    const rtAddress = s.address || '';
    const rtQr = s.retailUpiQr || '';
    const qrBlock = rtQr ? `<div style="flex-shrink:0;display:flex;flex-direction:column;align-items:center;gap:4px;margin-left:16px"><img src="${rtQr}" style="width:90px;height:90px;object-fit:contain;border:1.5px solid #bbf7d0;border-radius:8px;background:#f8fafc"/><div style="font-size:9px;color:#64748b;font-weight:600;letter-spacing:0.5px">SCAN TO PAY</div></div>` : '';

    billHTML = `
    <div class="print-doc">
      <!-- ═══ STORE HEADER ═══ -->
      <div class="print-header" style="padding-bottom:14px;border-bottom:2px solid #10b981;margin-bottom:0;display:flex;justify-content:space-between;align-items:flex-start">
        <div style="flex:1;min-width:0;text-align:center">
          <div class="print-store-name" style="font-size:22px;letter-spacing:-0.3px">${s.storeName || 'My Pharmacy'}</div>
          <div class="print-store-info" style="color:#10b981;font-weight:700;font-size:13px;margin:3px 0">${type}</div>
          ${rtShop ? `<div class="print-store-info"><strong>Shop:</strong> ${rtShop}${rtOwner ? ' &nbsp;|&nbsp; <strong>Owner:</strong> ' + rtOwner : ''}</div>` : ''}
          ${rtAddress ? `<div class="print-store-info">📍 ${rtAddress}</div>` : ''}
          <div class="print-store-info">${rtPhone ? '📞 ' + rtPhone : ''}${rtEmail ? (rtPhone ? ' &nbsp;|&nbsp; ' : '') + '✉ ' + rtEmail : ''}</div>
          <div class="print-store-info">${rtGstin ? 'GSTIN: <strong>' + rtGstin + '</strong>' : ''}${rtLicense ? (rtGstin ? ' &nbsp;|&nbsp; ' : '') + 'DL No: <strong>' + rtLicense + '</strong>' : ''}</div>
        </div>
        ${qrBlock}
      </div>

      <!-- ═══ BILL TITLE BAND ═══ -->
      <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:0 0 8px 8px;padding:8px 16px;display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;flex-wrap:wrap;gap:6px">
        <div style="font-size:15px;font-weight:800;color:#166534;letter-spacing:0.5px">TAX INVOICE / BILL</div>
        <div style="font-size:13px;font-weight:700;color:#374151">Bill No: <span style="color:#10b981;font-family:'JetBrains Mono',monospace">#${bill.billNo}</span> &nbsp;|&nbsp; Date: ${fmtDate(bill.date)}</div>
      </div>

      <!-- ═══ BILL META (Customer Details) ═══ -->
      <div class="print-meta" style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px 16px;margin-bottom:16px;display:grid;grid-template-columns:1fr 1fr;gap:6px 20px">
        <div><strong>Customer:</strong> ${bill.customer || 'Walk-in'}</div>
        <div><strong>Phone:</strong> ${bill.phone || '—'}</div>
        <div><strong>Doctor:</strong> ${bill.doctor || '—'}</div>
        <div><strong>Payment Mode:</strong> <span style="color:#10b981;font-weight:700">${bill.paymentMode}</span></div>
        ${bill.notes ? `<div style="grid-column:1/-1"><strong>Notes:</strong> ${bill.notes}</div>` : ''}
      </div>

      <!-- ═══ ITEMS TABLE ═══ -->
      <table class="print-items-table" style="border:1px solid #e2e8f0;border-radius:8px;overflow:hidden">
        <thead style="background:#f0fdf4">
          <tr>
            <th style="color:#166534">#</th>
            <th style="color:#166534">Medicine</th>
            <th style="color:#166534">Form</th>
            <th style="color:#166534">Qty</th>
            <th style="color:#166534">MRP</th>
            <th style="color:#166534">Disc%</th>
            <th style="color:#166534">GST</th>
            <th style="color:#166534">Amount</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>

      <!-- ═══ TOTALS ═══ -->
      <div class="print-totals" style="margin-top:0;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 8px 8px;padding:12px 16px;background:#fafafa">
        <div style="color:#64748b">Subtotal: ${cur2(bill.subtotal)}</div>
        <div style="color:#ef4444">Discount: -${cur2(bill.totalDiscount)}</div>
        <div style="color:#6366f1">GST: ${cur2(bill.totalGst)}</div>
        ${bill.roundOff ? `<div style="color:#94a3b8">Round Off: ${cur2(bill.roundOff)}</div>` : ''}
        <div class="print-grand-total" style="color:#10b981;border-top:2px solid #10b981;margin-top:8px;padding-top:8px">GRAND TOTAL: ${cur2(bill.grandTotal)}</div>
      </div>

      <!-- ═══ FOOTER ═══ -->
      <div class="print-footer" style="margin-top:18px;border-top:1px dashed #bbf7d0;padding-top:12px;color:#64748b">
        Thank you for choosing <strong>${s.storeName || 'our pharmacy'}</strong> · Get well soon! 💊<br/>
        Computer generated bill — no signature required · Licensed Pharmacy
        ${rtGstin ? `<br/>GSTIN: ${rtGstin}` : ''}${rtLicense ? ` | DL No: ${rtLicense}` : ''}
      </div>
    </div>`;
  }

  document.getElementById('bill-print-content').innerHTML = billHTML;
  openModal('bill-view-modal');
}

// ══════════════════════════════════════════════════
// SALES HISTORY
// ══════════════════════════════════════════════════
function renderHistory() {
  const q=(document.getElementById('hist-search')?.value||'').toLowerCase();
  const from=document.getElementById('hist-from')?.value||'';
  const to=document.getElementById('hist-to')?.value||'';
  const pay=document.getElementById('hist-payment')?.value||'';
  const isWholesale = (STATE.settings.storeType||'').trim() === 'Wholesale Pharma';

  const tbody    = document.getElementById(isWholesale ? 'history-tbody-wholesale' : 'history-tbody');
  const mobileEl = document.getElementById('history-mobile');

  // ── KEY FIX: filter bills by their store type so histories are separate ──
  const storeTypeFilter = isWholesale ? 'wholesale' : 'retail';
  let bills = STATE.bills.filter(b=>{
    // legacy bills without billStoreType: show in retail (non-wholesale) view only
    const bType = b.billStoreType || 'retail';
    if (bType !== storeTypeFilter) return false;
    const mq=!q||b.billNo.includes(q)||b.customer.toLowerCase().includes(q)||(b.doctor||'').toLowerCase().includes(q)||(b.phone||'').includes(q);
    return mq && (!from||b.date>=from) && (!to||b.date<=to) && (!pay||b.paymentMode===pay);
  });

  const sumBar=document.getElementById('hist-summary-bar');
  if (sumBar && bills.length>0) {
    const totalRev=bills.reduce((s,b)=>s+b.grandTotal,0), totalGst=bills.reduce((s,b)=>s+b.totalGst,0), totalDisc=bills.reduce((s,b)=>s+b.totalDiscount,0);
    sumBar.classList.remove('hidden');
    sumBar.innerHTML=`<div class="hist-sum-item">Bills: <strong>${bills.length}</strong></div><div class="hist-sum-item">Revenue: <strong>${cur(totalRev)}</strong></div><div class="hist-sum-item">GST: <strong>${cur(totalGst)}</strong></div><div class="hist-sum-item">Discount: <strong>${cur(totalDisc)}</strong></div>`;
  } else if (sumBar) sumBar.classList.add('hidden');

  const colspan = isWholesale ? 15 : 11;
  if (bills.length===0) {
    if (tbody) tbody.innerHTML=`<tr class="empty-row"><td colspan="${colspan}">No bills match the filter</td></tr>`;
    if (mobileEl) mobileEl.innerHTML='<div style="text-align:center;padding:28px;color:#94a3b8;font-style:italic">No bills match the filter</div>';
    return;
  }

  if (isWholesale) {
    if (tbody) tbody.innerHTML = bills.map(b => {
      const stockNames = b.items.map(it => it.name).join(', ');
      const wsGstin = b.wsGstin || STATE.settings.gstin || '—';
      const skGstin = b.shopkeeperGstin || '—';
      return `<tr>
        <td style="font-family:'JetBrains Mono',monospace;font-weight:700;color:#0ea5e9">${b.billNo}</td>
        <td style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#64748b">${wsGstin}</td>
        <td style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#7c3aed">${skGstin}</td>
        <td style="font-size:12px;white-space:nowrap">${fmtDate(b.date)}</td>
        <td><div style="font-weight:600">${b.shopName || b.customer}</div><div style="font-size:11px;color:#94a3b8">${b.phone||''}</div></td>
        <td style="font-size:12px">${b.customer || '—'}</td>
        <td style="font-family:'JetBrains Mono',monospace;font-size:12px">${b.phone || '—'}</td>
        <td style="font-size:12px;max-width:160px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${stockNames}">${stockNames}</td>
        <td style="text-align:center">${b.items.length} item${b.items.length!==1?'s':''}</td>
        <td style="font-family:'JetBrains Mono',monospace">${cur(b.subtotal)}</td>
        <td style="font-size:12px;color:#64748b">${cur(b.totalGst)}</td>
        <td style="color:#10b981;font-size:12px">-${cur(b.totalDiscount)}</td>
        <td style="font-family:'JetBrains Mono',monospace;font-weight:700;color:var(--accent)">${cur(b.grandTotal)}</td>
        <td><span class="badge ${b.paymentMode==='Cash'?'badge-green':b.paymentMode==='NEFT'?'badge-blue':'badge-blue'}">${b.paymentMode}</span></td>
        <td style="white-space:nowrap">
          <button class="btn-icon" onclick="showBillView(STATE.bills.find(x=>x.id==='${b.id}'))" title="View">&#128065;</button>
          <button class="btn-icon" onclick="(function(){showBillView(STATE.bills.find(x=>x.id==='${b.id}'));setTimeout(()=>window.print(),400)})()" title="Print">&#128424;</button>
          <button class="btn-icon" onclick="deleteBill('${b.id}')" title="Delete">&#128465;</button>
        </td>
      </tr>`;
    }).join('');
  } else {
    if (tbody) tbody.innerHTML = bills.map(b=>`
      <tr>
        <td style="font-family:'JetBrains Mono',monospace;font-weight:700;color:#0ea5e9">#${b.billNo}</td>
        <td style="font-size:12px">${fmtDate(b.date)}</td>
        <td>
          <div style="font-weight:600">${b.customer || 'Walk-in'}</div>
          <div style="font-size:11px;color:#94a3b8">${b.phone||''}</div>
          ${b.rtShop ? `<div style="font-size:10px;color:#10b981;font-weight:600">🏪 ${b.rtShop}</div>` : ''}
          ${b.rtGstin ? `<div style="font-size:10px;color:#6366f1">GSTIN: ${b.rtGstin}</div>` : ''}
        </td>
        <td style="font-size:12px">${b.doctor||'—'}</td>
        <td>${b.items.length} item${b.items.length!==1?'s':''}</td>
        <td style="font-family:'JetBrains Mono',monospace">${cur(b.subtotal)}</td>
        <td style="font-size:12px;color:#64748b">${cur(b.totalGst)}</td>
        <td style="color:#10b981;font-size:12px">-${cur(b.totalDiscount)}</td>
        <td style="font-family:'JetBrains Mono',monospace;font-weight:700;color:var(--accent)">${cur(b.grandTotal)}</td>
        <td><span class="badge ${b.paymentMode==='Cash'?'badge-green':b.paymentMode==='Insurance'?'badge-purple':b.paymentMode==='Credit'?'badge-red':'badge-blue'}">${b.paymentMode}</span></td>
        <td style="white-space:nowrap">
          <button class="btn-icon" onclick="showBillView(STATE.bills.find(x=>x.id==='${b.id}'))" title="View">&#128065;</button>
          <button class="btn-icon" onclick="(function(){showBillView(STATE.bills.find(x=>x.id==='${b.id}'));setTimeout(()=>window.print(),400)})()" title="Print">&#128424;</button>
          <button class="btn-icon" onclick="deleteBill('${b.id}')" title="Delete">&#128465;</button>
        </td>
      </tr>`).join('');
  }

  if (mobileEl) mobileEl.innerHTML = bills.map(b=>`
    <div class="m-card">
      <div class="m-card-hd">
        <div><span style="font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:700;background:#f0f9ff;color:#0369a1;padding:2px 8px;border-radius:20px">#${b.billNo}</span><div class="m-card-name" style="margin-top:4px">${b.customer || 'Walk-in'}</div>${b.rtShop&&!isWholesale?`<div style="font-size:11px;color:#10b981;font-weight:600">🏪 ${b.rtShop}</div>`:''}</div>
        <div style="text-align:right"><div style="font-family:'JetBrains Mono',monospace;font-weight:800;color:var(--accent);font-size:16px">${cur(b.grandTotal)}</div><span class="badge ${b.paymentMode==='Cash'?'badge-green':b.paymentMode==='Credit'?'badge-red':'badge-blue'}" style="margin-top:4px">${b.paymentMode}</span></div>
      </div>
      <div class="m-card-row"><span>Date</span><strong>${fmtDate(b.date)}</strong></div>
      <div class="m-card-row"><span>${isWholesale?'Owner Name':'Doctor'}</span><strong>${b.doctor||'—'}</strong></div>
      ${isWholesale?`<div class="m-card-row"><span>Stock Name</span><strong style="font-size:11px">${b.items.map(it=>it.name).join(', ')}</strong></div>`:''}
      ${!isWholesale&&b.rtGstin?`<div class="m-card-row"><span>GSTIN</span><strong style="font-size:11px;color:#6366f1">${b.rtGstin}</strong></div>`:''}
      <div class="m-card-row"><span>Items</span><strong>${b.items.length}</strong></div>
      <div class="m-card-row"><span>GST</span><strong style="font-family:'JetBrains Mono',monospace">${cur(b.totalGst)}</strong></div>
      ${b.totalDiscount>0?`<div class="m-card-row"><span>Discount</span><strong style="color:#10b981">-${cur(b.totalDiscount)}</strong></div>`:''}
      <div class="m-card-actions">
        <button class="act-edit" onclick="showBillView(STATE.bills.find(x=>x.id==='${b.id}'))">&#128065; View</button>
        <button class="act-stock" onclick="(function(){showBillView(STATE.bills.find(x=>x.id==='${b.id}'));setTimeout(()=>window.print(),400)})()">&#128424; Print</button>
        <button class="act-del" onclick="deleteBill('${b.id}')">&#128465; Delete</button>
      </div>
    </div>`).join('');
}

function clearHistoryFilters() { ['hist-search','hist-from','hist-to'].forEach(id=>{ const el=document.getElementById(id); if(el) el.value=''; }); document.getElementById('hist-payment').value=''; renderHistory(); }
function deleteBill(id) { if (!confirm('Delete this bill? Stock will NOT be restored.')) return; STATE.bills=STATE.bills.filter(b=>b.id!==id); saveState(); renderHistory(); toast('Bill deleted'); }
function exportCSV() {
  const isWholesale = (STATE.settings.storeType||'').trim() === 'Wholesale Pharma';
  let rows, dataRows;
  if (isWholesale) {
    rows = [['Bill No','GSTIN (Wholesaler)','GSTIN (Shopkeeper)','Date','Shop Name','Shopkeeper','Phone','Items','Subtotal','GST','Discount','Total','Payment']];
    dataRows = STATE.bills.map(b=>[b.billNo,b.wsGstin||'',b.shopkeeperGstin||'',b.date,b.shopName||'',b.customer,b.phone||'',b.items.length,b.subtotal,b.totalGst,b.totalDiscount,b.grandTotal,b.paymentMode]);
  } else {
    rows = [['Bill No','Date','Shop Name','Owner','GSTIN','DL No','Customer','Doctor','Items','Subtotal','GST','Discount','Total','Payment']];
    dataRows = STATE.bills.map(b=>[b.billNo,b.date,b.rtShop||'',b.rtOwner||'',b.rtGstin||'',b.rtLicense||'',b.customer,b.doctor||'',b.items.length,b.subtotal,b.totalGst,b.totalDiscount,b.grandTotal,b.paymentMode]);
  }
  rows = rows.concat(dataRows);
  const csv=rows.map(r=>r.map(v=>`"${v}"`).join(',')).join('\n');
  const a=document.createElement('a'); a.href='data:text/csv,'+encodeURIComponent(csv); a.download=`sales_${today()}.csv`; a.click(); toast('CSV exported ✓');
}

// ══════════════════════════════════════════════════
// ANALYSIS
// ══════════════════════════════════════════════════

// Mirror of getDashBills but also applies the analysis period filter.
// This ensures Analysis always reflects the same bill set as the Dashboard.
function getAnalysisBills() {
  const isWholesale = (STATE.settings.storeType || '').trim() === 'Wholesale Pharma';
  const typeKey   = isWholesale ? 'wholesale' : 'retail';
  const resets    = STATE.dashboardResets || {};
  const resetDate = resets[typeKey] || null;
  const fromDate  = daysAgo(analysisPeriod);
  return STATE.bills.filter(b => {
    const bType  = b.billStoreType || 'retail';
    const typeOk = isWholesale ? bType === 'wholesale' : bType !== 'wholesale';
    if (!typeOk) return false;
    if (resetDate && b.date < resetDate) return false;  // respect dashboard reset
    if (b.date < fromDate) return false;                 // respect period filter
    return true;
  });
}

// Shared helper: draw weekly-profit bar chart for Analysis section
// Works for both wholesale and retail — just pass different canvas/key IDs.
function _renderAnalysisProfitChart(bills, canvasId, chartKey, labelId, summaryId) {
  destroyChart(chartKey);
  const ctx = document.getElementById(canvasId); if (!ctx) return;
  const purchaseMap = {};
  STATE.products.forEach(p => { purchaseMap[p.id] = p.purchase || 0; });
  const now        = new Date();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const monthEnd   = new Date(now.getFullYear(), now.getMonth() + 1, 0);
  const monthName  = now.toLocaleDateString('en-IN', { month: 'long', year: 'numeric' });
  const labelEl    = document.getElementById(labelId);
  if (labelEl) labelEl.textContent = monthName;
  const weekProfit = [0, 0, 0, 0];
  bills.forEach(b => {
    const bd = new Date(b.date);
    if (bd < monthStart || bd > monthEnd) return;
    const day = bd.getDate();
    const wi  = day <= 7 ? 0 : day <= 14 ? 1 : day <= 21 ? 2 : 3;
    b.items.forEach(it => {
      const pp = purchaseMap[it.productId] || 0;
      weekProfit[wi] += (it.unitPrice - pp) * it.qty;
    });
  });
  const data      = weekProfit.map(v => +v.toFixed(2));
  const barColors = data.map(v => v >= 0 ? 'rgba(16,185,129,0.85)' : 'rgba(239,68,68,0.85)');
  chartInstances[chartKey] = new Chart(ctx, {
    type: 'bar',
    data: { labels: ['Week 1  (Days 1–7)', 'Week 2  (Days 8–14)', 'Week 3  (Days 15–21)', 'Week 4  (Days 22–end)'],
            datasets: [{ label: 'Profit (₹)', data, backgroundColor: barColors, borderRadius: 8, borderSkipped: false }] },
    options: { responsive: true,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => ' Profit: ' + (STATE.settings.currency || '₹') + c.parsed.y.toFixed(2) } } },
      scales: { y: { beginAtZero: true, grid: { color: '#f1f5f9' }, ticks: { callback: v => (STATE.settings.currency || '₹') + v } }, x: { grid: { display: false } } } }
  });
  const summaryEl = document.getElementById(summaryId);
  if (summaryEl) {
    const weekNames = ['Week 1', 'Week 2', 'Week 3', 'Week 4'];
    summaryEl.innerHTML = data.map((v, i) => {
      const color = v >= 0 ? '#10b981' : '#ef4444';
      const bg    = v >= 0 ? '#f0fdf4' : '#fef2f2';
      return `<div style="flex:1;min-width:120px;background:${bg};border-radius:10px;padding:10px 14px;border:1px solid ${v>=0?'#bbf7d0':'#fecaca'}">
        <div style="font-size:11px;color:#94a3b8;font-weight:600">${weekNames[i]}</div>
        <div style="font-size:16px;font-weight:700;color:${color};font-family:'JetBrains Mono',monospace">${(STATE.settings.currency || '₹')}${v.toFixed(2)}</div>
      </div>`;
    }).join('');
  }
}

function renderAnalysis() {
  const isWholesale = (STATE.settings.storeType || '').trim() === 'Wholesale Pharma';
  const dashMeta    = getDashboardLabel();
  const filtered    = getAnalysisBills();   // type-filtered + reset-aware + period-filtered
  const fromDate    = daysAgo(analysisPeriod);

  // ── Type Banner (same style as Dashboard banner) ──────
  const bannerEl = document.getElementById('analysis-type-banner');
  if (bannerEl) {
    const resets    = STATE.dashboardResets || {};
    const typeKey   = isWholesale ? 'wholesale' : 'retail';
    const resetDate = resets[typeKey] || null;
    bannerEl.innerHTML = `
      <span style="display:inline-flex;align-items:center;gap:6px;background:${dashMeta.color}18;border:1.5px solid ${dashMeta.color}40;color:${dashMeta.color};border-radius:30px;padding:4px 14px;font-size:12px;font-weight:700;letter-spacing:.3px">
        ${dashMeta.icon} ${dashMeta.label} Analysis
        ${resetDate ? `<span style="font-weight:400;opacity:.75;margin-left:4px">· since ${fmtDate(resetDate)}</span>` : ''}
      </span>`;
  }

  // ── Show/hide mode-specific chart rows ────────────────
  document.getElementById('analysis-wholesale-extra')?.classList.toggle('hidden', !isWholesale);
  document.getElementById('analysis-retail-extra')?.classList.toggle('hidden', isWholesale);

  // ── Aggregate bill data ───────────────────────────────
  const totalRev  = filtered.reduce((s, b) => s + b.grandTotal, 0);
  const avgBill   = filtered.length > 0 ? totalRev / filtered.length : 0;
  const prodSales = {}, catSales = {};
  filtered.forEach(b => b.items.forEach(it => {
    if (!prodSales[it.name]) prodSales[it.name] = { units: 0, revenue: 0, category: it.category };
    prodSales[it.name].units   += it.qty;
    prodSales[it.name].revenue += it.lineTotal;
    const cn = getCatName(it.category);
    catSales[cn] = (catSales[cn] || 0) + it.lineTotal;
  }));
  const topProduct = Object.entries(prodSales).sort((a, b) => b[1].revenue - a[1].revenue)[0];

  // ── Stat Cards ────────────────────────────────────────
  const statsEl = document.getElementById('analysis-stats');
  if (statsEl) {
    if (isWholesale) {
      const uniqueRetailers = new Set(filtered.map(b => b.customer)).size;
      statsEl.innerHTML = `
        <div class="stat-card" style="--stat-color:#0ea5e9;--stat-color2:#38bdf8"><div class="stat-icon">🧾</div><div class="stat-value">${filtered.length}</div><div class="stat-label">Total Invoices</div></div>
        <div class="stat-card" style="--stat-color:#10b981;--stat-color2:#34d399"><div class="stat-icon">₹</div><div class="stat-value">${cur(totalRev)}</div><div class="stat-label">Wholesale Revenue</div></div>
        <div class="stat-card" style="--stat-color:#f97316;--stat-color2:#fb923c"><div class="stat-icon">📦</div><div class="stat-value">${cur(avgBill)}</div><div class="stat-label">Avg Invoice Value</div></div>
        <div class="stat-card" style="--stat-color:#8b5cf6;--stat-color2:#a78bfa"><div class="stat-icon">🏪</div><div class="stat-value">${uniqueRetailers}</div><div class="stat-label">Active Retailers</div></div>`;
    } else {
      statsEl.innerHTML = `
        <div class="stat-card" style="--stat-color:#0ea5e9;--stat-color2:#38bdf8"><div class="stat-icon">🧾</div><div class="stat-value">${filtered.length}</div><div class="stat-label">Total Bills</div></div>
        <div class="stat-card" style="--stat-color:#10b981;--stat-color2:#34d399"><div class="stat-icon">₹</div><div class="stat-value">${cur(totalRev)}</div><div class="stat-label">Total Revenue</div></div>
        <div class="stat-card" style="--stat-color:#f97316;--stat-color2:#fb923c"><div class="stat-icon">📈</div><div class="stat-value">${cur(avgBill)}</div><div class="stat-label">Avg Bill Value</div></div>
        <div class="stat-card" style="--stat-color:#8b5cf6;--stat-color2:#a78bfa"><div class="stat-icon">🏆</div><div class="stat-value" style="font-size:16px">${topProduct ? topProduct[0].split(' ').slice(0, 2).join(' ') : '—'}</div><div class="stat-label">Top Medicine</div></div>`;
    }
  }

  // ── Destroy all analysis charts before redrawing ──────
  destroyChart('anRev'); destroyChart('anCat'); destroyChart('anTop'); destroyChart('anPay');
  destroyChart('anCust'); destroyChart('anProfit'); destroyChart('anProfitRetail');
  const COLORS = ['#0ea5e9','#10b981','#f97316','#8b5cf6','#f59e0b','#ec4899','#14b8a6','#ef4444','#6366f1'];

  // ── Chart 1: Daily Revenue (line) ─────────────────────
  const titleC1 = document.getElementById('an-chart1-title');
  if (titleC1) titleC1.textContent = isWholesale ? 'Daily Wholesale Revenue' : 'Daily Revenue';
  const rctx = document.getElementById('chart-analysis-revenue');
  if (rctx) {
    const days = Math.min(analysisPeriod, 30), labels = [], data = [];
    for (let i = days - 1; i >= 0; i--) {
      const d = daysAgo(i);
      if (d < fromDate) continue;
      labels.push(new Date(d).toLocaleDateString('en-IN', { day: 'numeric', month: 'short' }));
      data.push(filtered.filter(b => b.date === d).reduce((s, b) => s + b.grandTotal, 0));
    }
    chartInstances['anRev'] = new Chart(rctx, {
      type: 'line',
      data: { labels, datasets: [{ label: 'Revenue', data,
          borderColor: dashMeta.color, backgroundColor: dashMeta.color + '1a',
          tension: 0.4, fill: true, pointBackgroundColor: dashMeta.color, pointRadius: 4 }] },
      options: { responsive: true, plugins: { legend: { display: false } },
        scales: { y: { beginAtZero: true, grid: { color: '#f1f5f9' }, ticks: { callback: v => STATE.settings.currency + v } }, x: { grid: { display: false } } } }
    });
  }

  // ── Chart 2: Category Doughnut (both modes) ───────────
  const cctx = document.getElementById('chart-analysis-category');
  if (cctx) {
    const entries = Object.entries(catSales).sort((a, b) => b[1] - a[1]);
    chartInstances['anCat'] = new Chart(cctx, {
      type: 'doughnut',
      data: { labels: entries.map(([k]) => k), datasets: [{ data: entries.map(([, v]) => +v.toFixed(2)), backgroundColor: COLORS, borderWidth: 2, borderColor: '#fff' }] },
      options: { responsive: true, cutout: '62%', plugins: { legend: { position: 'bottom', labels: { font: { size: 11 }, padding: 10 } } } }
    });
  }

  // ── Chart 3: Top Medicines / Products (horizontal bar) ─
  const titleC3 = document.getElementById('an-chart3-title');
  if (titleC3) titleC3.textContent = isWholesale ? 'Top Products by Volume' : 'Top Medicines';
  const tctx = document.getElementById('chart-analysis-top');
  if (tctx) {
    const sorted = Object.entries(prodSales).sort((a, b) => b[1].units - a[1].units).slice(0, 8);
    chartInstances['anTop'] = new Chart(tctx, {
      type: 'bar',
      data: { labels: sorted.map(([k]) => k.length > 22 ? k.slice(0, 22) + '…' : k),
              datasets: [{ label: 'Units', data: sorted.map(([, v]) => v.units), backgroundColor: COLORS, borderRadius: 6, borderSkipped: false }] },
      options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } },
        scales: { x: { beginAtZero: true, grid: { color: '#f1f5f9' } }, y: { grid: { display: false } } } }
    });
  }

  // ── Chart 4: Payment Mode Split (both modes) ──────────
  const pctx = document.getElementById('chart-analysis-payment');
  if (pctx) {
    const pay = {}; filtered.forEach(b => { pay[b.paymentMode] = (pay[b.paymentMode] || 0) + b.grandTotal; });
    const entries = Object.entries(pay);
    chartInstances['anPay'] = new Chart(pctx, {
      type: 'pie',
      data: { labels: entries.map(([k, v]) => `${k} (${cur(v)})`),
              datasets: [{ data: entries.map(([, v]) => +v.toFixed(2)), backgroundColor: COLORS, borderWidth: 2, borderColor: '#fff' }] },
      options: { responsive: true, plugins: { legend: { position: 'bottom', labels: { font: { size: 11 }, padding: 10 } } } }
    });
  }

  // ── Wholesale-only charts ─────────────────────────────
  if (isWholesale) {
    // Chart 5W: Top Customers / Retailers (horizontal bar)
    const custCtx = document.getElementById('chart-analysis-customers');
    if (custCtx) {
      const custRev = {};
      filtered.forEach(b => { custRev[b.customer] = (custRev[b.customer] || 0) + b.grandTotal; });
      const topCust = Object.entries(custRev).sort((a, b) => b[1] - a[1]).slice(0, 8);
      chartInstances['anCust'] = new Chart(custCtx, {
        type: 'bar',
        data: { labels: topCust.map(([k]) => k.length > 22 ? k.slice(0, 22) + '…' : k),
                datasets: [{ label: 'Revenue', data: topCust.map(([, v]) => +v.toFixed(2)), backgroundColor: COLORS, borderRadius: 6, borderSkipped: false }] },
        options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } },
          scales: { x: { beginAtZero: true, grid: { color: '#f1f5f9' }, ticks: { callback: v => STATE.settings.currency + v } }, y: { grid: { display: false } } } }
      });
    }
    // Chart 6W: Weekly Profit for wholesale bills
    _renderAnalysisProfitChart(filtered, 'chart-analysis-profit', 'anProfit', 'an-profit-month-label', 'an-profit-week-summary');
  } else {
    // Chart 5R: Weekly Profit for retail/hospital/medical/ayurvedic bills
    _renderAnalysisProfitChart(filtered, 'chart-analysis-profit-retail', 'anProfitRetail', 'an-profit-month-label-retail', 'an-profit-week-summary-retail');
  }

  // ── Performance Table (both modes) ───────────────────
  const tableTitleEl = document.getElementById('an-table-title');
  if (tableTitleEl) tableTitleEl.textContent = isWholesale ? 'Product Sales Performance' : 'Sales Performance';
  const tbody = document.getElementById('analysis-prod-tbody');
  if (tbody) {
    const sorted = Object.entries(prodSales).sort((a, b) => b[1].revenue - a[1].revenue);
    if (!sorted.length) { tbody.innerHTML = '<tr class="empty-row"><td colspan="6">No sales in this period</td></tr>'; return; }
    tbody.innerHTML = sorted.map(([name, data]) => {
      const contrib = totalRev > 0 ? ((data.revenue / totalRev) * 100).toFixed(1) : '0';
      return `<tr>
        <td style="font-weight:600">${name}</td>
        <td><span class="badge badge-blue">${getCatName(data.category)}</span></td>
        <td style="font-family:'JetBrains Mono',monospace;font-weight:700">${data.units}</td>
        <td style="font-family:'JetBrains Mono',monospace;color:var(--accent);font-weight:700">${cur(data.revenue)}</td>
        <td style="font-family:'JetBrains Mono',monospace">${cur(data.revenue / data.units)}</td>
        <td><div style="display:flex;align-items:center;gap:8px"><div style="flex:1;background:#f1f5f9;border-radius:99px;height:6px"><div style="width:${contrib}%;background:linear-gradient(90deg,#0ea5e9,#38bdf8);height:6px;border-radius:99px"></div></div><span style="font-size:12px;font-weight:700;color:#0ea5e9;min-width:38px">${contrib}%</span></div></td>
      </tr>`;
    }).join('');
  }
}

// ══════════════════════════════════════════════════
// EXPIRY TRACKER
// ══════════════════════════════════════════════════
function renderExpiryTracker(filter) {
  const alertDays = STATE.settings.expiryAlertDays || 90;
  const expired = STATE.products.filter(p=>expiryDaysLeft(p.expiry)<0);
  const exp30 = STATE.products.filter(p=>{ const d=expiryDaysLeft(p.expiry); return d>=0&&d<=30; });
  const exp60 = STATE.products.filter(p=>{ const d=expiryDaysLeft(p.expiry); return d>=0&&d<=60; });
  const exp90 = STATE.products.filter(p=>{ const d=expiryDaysLeft(p.expiry); return d>=0&&d<=90; });

  // Send expiry alert email if there are expired or critical items (throttle: once per session per page load)
  if (!renderExpiryTracker._alerted && STATE.settings.email && (expired.length > 0 || exp30.length > 0)) {
    renderExpiryTracker._alerted = true;
    const lines = [];
    if (expired.length) lines.push(`EXPIRED (${expired.length}):\n${expired.map(p=>`  • ${p.name} (Batch: ${p.sku||'—'}, Stock: ${p.stock})`).join('\n')}`);
    if (exp30.length)   lines.push(`Expiring within 30 days (${exp30.length}):\n${exp30.map(p=>`  • ${p.name} — ${fmtMonth(p.expiry)} (Stock: ${p.stock})`).join('\n')}`);
    sendAlertEmail(
      `📅 Expiry Alert — ${STATE.settings.storeName}`,
      `Hello,\n\nPlease review the following medicines that require urgent attention:\n\n${lines.join('\n\n')}\n\nPlease take necessary action immediately.\n\nRegards,\n${STATE.settings.storeName}`
    );
  }

  const statsEl=document.getElementById('expiry-stats');
  if (statsEl) statsEl.innerHTML=`
    <div class="stat-card" style="--stat-color:#ef4444;--stat-color2:#f87171"><div class="stat-icon">⛔</div><div class="stat-value">${expired.length}</div><div class="stat-label">Expired</div></div>
    <div class="stat-card" style="--stat-color:#f97316;--stat-color2:#fb923c"><div class="stat-icon">🔴</div><div class="stat-value">${exp30.length}</div><div class="stat-label">Within 30 Days</div></div>
    <div class="stat-card" style="--stat-color:#f59e0b;--stat-color2:#fbbf24"><div class="stat-icon">🟡</div><div class="stat-value">${exp60.length}</div><div class="stat-label">Within 60 Days</div></div>
    <div class="stat-card" style="--stat-color:#10b981;--stat-color2:#34d399"><div class="stat-icon">🟢</div><div class="stat-value">${exp90.length}</div><div class="stat-label">Within 90 Days</div></div>`;

  let prods, title;
  if (filter==='expired') { prods=expired; title='Expired Medicines'; }
  else if (filter==='30') { prods=exp30; title='Expiring Within 30 Days'; }
  else if (filter==='60') { prods=exp60; title='Expiring Within 60 Days'; }
  else if (filter==='90') { prods=exp90; title='Expiring Within 90 Days'; }
  else { prods=[...STATE.products].sort((a,b)=>expiryDaysLeft(a.expiry)-expiryDaysLeft(b.expiry)); title='All Medicines by Expiry'; }

  const titleEl=document.getElementById('expiry-table-title'); if(titleEl) titleEl.textContent=title;
  const tbody=document.getElementById('expiry-tbody'); const mobileEl=document.getElementById('expiry-mobile');

  if (!prods.length) {
    if (tbody) tbody.innerHTML='<tr class="empty-row"><td colspan="9">No medicines in this category</td></tr>';
    if (mobileEl) mobileEl.innerHTML='<div style="text-align:center;padding:28px;color:#94a3b8;font-style:italic">✓ No medicines in this category</div>'; return;
  }

  if (tbody) tbody.innerHTML = prods.map((p,i)=>{
    const eb=getExpiryBadge(p.expiry);
    return `<tr>
      <td style="color:#94a3b8">${i+1}</td>
      <td><div style="font-weight:600">${p.name}</div><div style="font-size:11px;color:#94a3b8">${p.brand||''}</div></td>
      <td><span class="badge badge-blue">${getCatName(p.category)}</span></td>
      <td style="font-family:'JetBrains Mono',monospace;font-size:11px">${p.sku||'—'}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-size:12px">${fmtMonth(p.expiry)||'—'}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-weight:700;color:${p.stock===0?'#ef4444':'#64748b'}">${p.stock}</td>
      <td style="font-family:'JetBrains Mono',monospace">${cur(p.sale)}</td>
      <td><span class="badge ${eb.cls}">${eb.label}</span></td>
      <td style="white-space:nowrap">
        <button class="btn-icon" onclick="editProduct('${p.id}')">✏️</button>
        <button class="btn-icon" onclick="quickStockEdit('${p.id}')">📦</button>
        <button class="btn-icon" onclick="deleteProduct('${p.id}')">🗑️</button>
      </td>
    </tr>`;
  }).join('');

  if (mobileEl) mobileEl.innerHTML = prods.map(p=>{
    const eb=getExpiryBadge(p.expiry);
    return `<div class="m-card">
      <div class="m-card-hd"><div class="m-card-name">${p.name}</div><span class="badge ${eb.cls}">${eb.label}</span></div>
      <div class="m-card-row"><span>Category</span><strong>${getCatName(p.category)}</strong></div>
      <div class="m-card-row"><span>Batch</span><strong style="font-family:'JetBrains Mono',monospace">${p.sku||'—'}</strong></div>
      <div class="m-card-row"><span>Expiry</span><strong style="font-family:'JetBrains Mono',monospace">${fmtMonth(p.expiry)||'—'}</strong></div>
      <div class="m-card-row"><span>Stock</span><strong style="font-family:'JetBrains Mono',monospace;color:${p.stock===0?'#ef4444':'#64748b'}">${p.stock} units</strong></div>
      <div class="m-card-row"><span>MRP</span><strong style="font-family:'JetBrains Mono',monospace">${cur(p.sale)}</strong></div>
      ${p.brand?`<div class="m-card-row"><span>Manufacturer</span><strong>${p.brand}</strong></div>`:''}
      <div class="m-card-actions">
        <button class="act-edit" onclick="editProduct('${p.id}')">✏️ Edit</button>
        <button class="act-stock" onclick="quickStockEdit('${p.id}')">📦 Stock</button>
        <button class="act-del" onclick="deleteProduct('${p.id}')">🗑 Delete</button>
      </div>
    </div>`;
  }).join('');
}

// ══════════════════════════════════════════════════
// CATEGORIES
// ══════════════════════════════════════════════════
function focusCatName() { document.getElementById('cat-name')?.focus(); }
function saveCategory() {
  const name=document.getElementById('cat-name')?.value.trim(); if (!name) { toast('Name required','err'); return; }
  const desc=document.getElementById('cat-desc')?.value.trim()||'';
  const editId=document.getElementById('cat-edit-id')?.value;
  if (editId) { const idx=STATE.categories.findIndex(c=>c.id===editId); STATE.categories[idx]={...STATE.categories[idx],name,desc}; document.getElementById('cat-edit-id').value=''; toast('Category updated ✓'); }
  else { if (STATE.categories.find(c=>c.name.toLowerCase()===name.toLowerCase())) { toast('Category already exists','err'); return; } STATE.categories.push({id:uid(),name,desc}); toast('Category added ✓'); }
  document.getElementById('cat-name').value=''; document.getElementById('cat-desc').value='';
  saveState(); renderCategories(); populateCategorySelects();
}
function renderCategories() {
  const list=document.getElementById('categories-list'); const stats=document.getElementById('cat-stats'); if (!list) return;
  if (!STATE.categories.length) { list.innerHTML='<p style="color:#94a3b8;font-style:italic;text-align:center;padding:20px">No categories yet</p>'; return; }
  list.innerHTML=STATE.categories.map(c=>{ const count=STATE.products.filter(p=>p.category===c.id).length;
    return `<div class="cat-item"><div><div class="cat-item-name">${c.name}</div>${c.desc?`<div class="cat-item-desc">${c.desc}</div>`:''}<div style="font-size:11px;color:#94a3b8;margin-top:2px">${count} medicine${count!==1?'s':''}</div></div><div style="display:flex;gap:4px"><button class="btn-icon" onclick="editCategory('${c.id}')">✏️</button><button class="btn-icon" onclick="deleteCategory('${c.id}')">🗑️</button></div></div>`;
  }).join('');
  if (stats) {
    const catRev={}; STATE.bills.forEach(b=>b.items.forEach(it=>{ const cn=getCatName(it.category); catRev[cn]=(catRev[cn]||0)+it.lineTotal; }));
    const totalRev=Object.values(catRev).reduce((s,v)=>s+v,0);
    stats.innerHTML=STATE.categories.map(c=>{ const rev=catRev[c.name]||0; const pct=totalRev>0?((rev/totalRev)*100).toFixed(1):'0'; const cnt=STATE.products.filter(p=>p.category===c.id).length;
      return `<div style="padding:12px;background:#f8fafc;border-radius:10px;border:1px solid var(--border)"><div style="display:flex;justify-content:space-between;margin-bottom:6px"><div style="font-weight:600;font-size:13.5px">${c.name}</div><div style="font-family:'JetBrains Mono',monospace;font-size:12px;color:var(--accent);font-weight:700">${cur(rev)}</div></div><div style="display:flex;justify-content:space-between;font-size:11px;color:#94a3b8;margin-bottom:6px"><span>${cnt} medicines</span><span>${pct}% of revenue</span></div><div style="background:#e2e8f0;border-radius:99px;height:5px"><div style="width:${pct}%;background:linear-gradient(90deg,#0ea5e9,#38bdf8);height:5px;border-radius:99px"></div></div></div>`;
    }).join('');
  }
}
function editCategory(id) { const c=STATE.categories.find(x=>x.id===id); if(!c) return; document.getElementById('cat-name').value=c.name; document.getElementById('cat-desc').value=c.desc||''; document.getElementById('cat-edit-id').value=id; document.getElementById('cat-name').focus(); }
function deleteCategory(id) {
  const count=STATE.products.filter(p=>p.category===id).length;
  if (count>0) { toast(`Cannot delete: ${count} medicine(s) use this category`,'err'); return; }
  if (!confirm('Delete this category?')) return;
  STATE.categories=STATE.categories.filter(c=>c.id!==id); saveState(); renderCategories(); populateCategorySelects(); toast('Category deleted');
}

// ══════════════════════════════════════════════════
// SETTINGS
// ══════════════════════════════════════════════════
function loadSettingsForm() {
  const s=STATE.settings;
  const set=(id,val)=>{ const el=document.getElementById(id); if(el) el.value=val||''; };
  set('set-name',s.storeName); set('set-type',s.storeType); set('set-address',s.address);
  set('set-phone',s.phone); set('set-email',s.email); set('set-license',s.license);
  set('set-gstin',s.gstin); set('set-gst',s.defaultGst); set('set-currency',s.currency);
  set('set-low-stock',s.lowStockThreshold); set('set-expiry-days',s.expiryAlertDays||90);
  // New fields
  set('set-supplier-name',s.supplierName); set('set-wholesaler',s.wholesaler);
  set('set-owner-name',s.ownerName); set('set-wholesaler-id',s.wholesalerId);
  set('set-shop-name',s.shopName); set('set-retailer-owner',s.retailerOwner);
  // Build app-info panel (QR blocks live here — must render BEFORE toggling visibility)
  const info=document.getElementById('app-info');
  if (info) info.innerHTML=`<div style="display:grid;gap:6px">
    <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #f1f5f9"><span>Total Medicines</span><strong>${STATE.products.length}</strong></div>
    <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #f1f5f9"><span>Categories</span><strong>${STATE.categories.length}</strong></div>
    <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #f1f5f9"><span>Total Bills</span><strong>${STATE.bills.length}</strong></div>
    <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #f1f5f9"><span>Expired Medicines</span><strong style="color:#ef4444">${STATE.products.filter(p=>expiryDaysLeft(p.expiry)<0).length}</strong></div>
    <div style="display:flex;justify-content:space-between;padding:6px 0"><span>Total Revenue</span><strong style="color:#10b981">${cur(STATE.bills.reduce((s,b)=>s+b.grandTotal,0))}</strong></div>
  </div>
  <!-- QR Code Upload Section -->
  <div style="margin-top:18px;padding-top:14px;border-top:2px dashed #e2e8f0">
    <div style="font-size:13px;font-weight:700;color:#0f172a;margin-bottom:4px">📲 UPI QR Code</div>
    <div style="font-size:11px;color:#94a3b8;margin-bottom:14px">Upload your UPI payment QR code. It will appear on bills and in the billing panel.</div>

    <!-- Wholesale QR — shown only when Wholesale Pharma is selected -->
    <div id="qr-wholesale-block" class="hidden" style="margin-bottom:16px">
      <div style="font-size:11px;font-weight:700;color:#0ea5e9;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px">Wholesale Pharma QR</div>
      <div id="qr-ws-preview" style="margin-bottom:8px"></div>
      <label style="display:inline-flex;align-items:center;gap:8px;cursor:pointer;background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:8px 14px;font-size:12px;font-weight:600;color:#0369a1">
        <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
        Upload QR Image
        <input type="file" accept="image/*" class="hidden" onchange="handleQrUpload(event,'wholesale')"/>
      </label>
      <button onclick="removeQr('wholesale')" class="btn-danger-xs" style="margin-left:8px;font-size:11px">Remove</button>
    </div>

    <!-- Retail QR — shown only when Retail / Hospital / Medical / Ayurvedic is selected -->
    <div id="qr-retail-block" class="hidden">
      <div style="font-size:11px;font-weight:700;color:#10b981;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px">Retail / Hospital / Medical / Ayurvedic QR</div>
      <div id="qr-rt-preview" style="margin-bottom:8px"></div>
      <label style="display:inline-flex;align-items:center;gap:8px;cursor:pointer;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:8px 14px;font-size:12px;font-weight:600;color:#166534">
        <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
        Upload QR Image
        <input type="file" accept="image/*" class="hidden" onchange="handleQrUpload(event,'retail')"/>
      </label>
      <button onclick="removeQr('retail')" class="btn-danger-xs" style="margin-left:8px;font-size:11px">Remove</button>
    </div>
  </div>`;
  // Now that QR blocks exist in the DOM, toggle visibility and populate previews
  togglePharmacyTypeFields();
  _renderQrPreviews();
  checkPharmacyTypeCredit();
}

function _renderQrPreviews() {
  const s = STATE.settings;
  const wsEl = document.getElementById('qr-ws-preview');
  const rtEl = document.getElementById('qr-rt-preview');
  if (wsEl) wsEl.innerHTML = s.wholesaleUpiQr
    ? `<img src="${s.wholesaleUpiQr}" style="width:120px;height:120px;object-fit:contain;border:2px solid #bae6fd;border-radius:10px;background:#f8fafc;display:block"/>`
    : '<div style="font-size:11px;color:#94a3b8;font-style:italic">No QR uploaded</div>';
  if (rtEl) rtEl.innerHTML = s.retailUpiQr
    ? `<img src="${s.retailUpiQr}" style="width:120px;height:120px;object-fit:contain;border:2px solid #bbf7d0;border-radius:10px;background:#f8fafc;display:block"/>`
    : '<div style="font-size:11px;color:#94a3b8;font-style:italic">No QR uploaded</div>';
}

function handleQrUpload(event, type) {
  const file = event.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = e => {
    if (type === 'wholesale') {
      STATE.settings.wholesaleUpiQr = e.target.result;
    } else {
      STATE.settings.retailUpiQr = e.target.result;
    }
    saveState();
    _renderQrPreviews();
    updateBillingQrPanel();
    toast('QR Code uploaded ✓');
  };
  reader.readAsDataURL(file);
  event.target.value = '';
}

function removeQr(type) {
  if (!confirm('Remove this QR Code?')) return;
  if (type === 'wholesale') STATE.settings.wholesaleUpiQr = '';
  else STATE.settings.retailUpiQr = '';
  saveState();
  _renderQrPreviews();
  updateBillingQrPanel();
  toast('QR Code removed');
}

// Update the QR panel shown in the billing section
function updateBillingQrPanel() {
  const isWholesale = (STATE.settings.storeType || '').trim() === 'Wholesale Pharma';
  const qr = isWholesale ? STATE.settings.wholesaleUpiQr : STATE.settings.retailUpiQr;
  const panel = document.getElementById('billing-qr-panel');
  if (!panel) return;
  if (qr) {
    panel.innerHTML = `
      <div style="text-align:center;padding:14px 10px;border:1.5px dashed #94a3b8;border-radius:12px;background:#f8fafc;margin-top:14px">
        <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.8px;color:#64748b;margin-bottom:10px">📲 SCAN HERE (QR CODE)</div>
        <img src="${qr}" style="width:130px;height:130px;object-fit:contain;border-radius:8px;display:block;margin:0 auto"/>
        <div style="font-size:10px;color:#94a3b8;margin-top:8px">Scan to pay via UPI</div>
      </div>`;
    panel.classList.remove('hidden');
  } else {
    panel.innerHTML = '';
    panel.classList.add('hidden');
  }
}
function saveSettings() {
  const get=id=>document.getElementById(id)?.value.trim()||'';
  const prevEmail    = STATE.settings.email || '';
  const prevType     = STATE.settings.storeType || '';
  const newEmail     = get('set-email');
  const newType      = get('set-type') || 'Retail Pharmacy';
  // Preserve QR codes — they are saved separately via handleQrUpload
  const wholesaleUpiQr = STATE.settings.wholesaleUpiQr || '';
  const retailUpiQr    = STATE.settings.retailUpiQr    || '';
  STATE.settings = {
    storeName:newType === prevType ? (get('set-name')||'My Pharmacy') : (get('set-name')||'My Pharmacy'),
    storeType:newType,
    address:get('set-address'), phone:get('set-phone'), email:newEmail,
    license:get('set-license'), gstin:get('set-gstin'),
    defaultGst:parseFloat(get('set-gst'))||12, currency:get('set-currency')||'₹',
    lowStockThreshold:parseInt(get('set-low-stock'))||10, expiryAlertDays:parseInt(get('set-expiry-days'))||90,
    // Wholesale
    supplierName:get('set-supplier-name'), wholesaler:get('set-wholesaler'),
    ownerName:get('set-owner-name'), wholesalerId:get('set-wholesaler-id'),
    // Retail
    shopName:get('set-shop-name'), retailerOwner:get('set-retailer-owner'),
    // QR Codes (preserve)
    wholesaleUpiQr, retailUpiQr
  };
  saveState(); applyBranding(); checkPharmacyTypeCredit(); togglePharmacyTypeFields(); updateBillingLayout(); updateBillingQrPanel();
  // If pharmacy type changed, refresh dashboard immediately with correct data
  if (newType !== prevType) {
    const dashMeta = getDashboardLabel();
    toast(`Switched to ${dashMeta.label} mode ✓ — Dashboard updated`, 'ok');
    // Re-render if dashboard is currently visible
    if (document.getElementById('page-dashboard')?.classList.contains('active')) {
      renderDashboard();
    }
  } else {
    toast('Settings saved ✓');
  }
  // Send notification if email is new/changed
  if (newEmail && newEmail !== prevEmail) {
    sendThankYouEmail(newEmail);
  }
}
// ══════════════════════════════════════════════════════════
//  IMPORT / EXPORT  —  Modal-based system
// ══════════════════════════════════════════════════════════

// ── State for import modal ────────────────────────────────
let _importType     = null;  // 'medicines' | 'sales' | 'credits' | 'olddata'
let _importFile     = null;  // File object
let _importAccept   = '';    // accept attribute string

// ── Export modal ──────────────────────────────────────────
function openExportModal()  { el('export-modal').classList.remove('hidden'); }
function closeExportModal() { el('export-modal').classList.add('hidden'); }

// ── Import modal ──────────────────────────────────────────
function openImportModal() {
  // Reset state
  _importType = null;
  _importFile = null;
  document.querySelectorAll('.import-type-card').forEach(c => {
    c.classList.remove('selected');
    c.querySelector('.import-type-check')?.classList.add('hidden');
  });
  el('import-file-area').classList.add('hidden');
  el('import-file-name').classList.add('hidden');
  el('import-confirm-btn').classList.add('hidden');
  el('import-file-input').value = '';
  el('import-step1').classList.remove('hidden');
  el('import-step2').classList.add('hidden');

  // Show current pharmacy type notice
  const storeType = (STATE.settings.storeType || 'Retail Pharmacy').trim();
  const notice = el('import-partition-notice');
  if (notice) {
    notice.innerHTML = `<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold"
      style="background:#eef2ff;color:#6366f1">
      🏥 Active pharmacy: <strong>${storeType}</strong>
    </span>&nbsp; Imported data will be saved for this pharmacy type.`;
  }

  el('import-modal').classList.remove('hidden');
}
function closeImportModal() { el('import-modal').classList.add('hidden'); }

function selectImportType(type) {
  _importType = type;
  _importFile = null;

  // Update card selection UI
  document.querySelectorAll('.import-type-card').forEach(c => {
    const isThis = c.dataset.type === type;
    c.classList.toggle('selected', isThis);
    c.querySelector('.import-type-check')?.classList.toggle('hidden', !isThis);
  });

  // Show file drop area with correct accept hint
  el('import-file-area').classList.remove('hidden');
  el('import-file-name').classList.add('hidden');
  el('import-confirm-btn').classList.add('hidden');

  const hints = {
    medicines: '.xlsx or .csv files only',
    sales:     '.xlsx or .csv files only',
    credits:   '.xlsx or .csv files only',
    olddata:   '.json files only (previously exported from this app)',
  };
  const accepts = {
    medicines: '.xlsx,.csv',
    sales:     '.xlsx,.csv',
    credits:   '.xlsx,.csv',
    olddata:   '.json',
  };
  _importAccept = accepts[type];
  el('import-accept-hint').textContent = hints[type];
  el('import-file-input').setAttribute('accept', _importAccept);
}

function handleImportDrop(event) {
  event.preventDefault();
  el('import-dropzone').classList.remove('dragover');
  const file = event.dataTransfer.files[0];
  if (file) processImportFile(file);
}

function handleImportFile(event) {
  const file = event.target.files[0];
  if (file) processImportFile(file);
}

function processImportFile(file) {
  if (!_importType) { toast('Please select a data type first', 'err'); return; }

  const ext = file.name.split('.').pop().toLowerCase();
  const allowedSpreadsheet = ['xlsx', 'csv'];
  const allowedJson        = ['json'];

  // ── Validate file type against selected import type ──────
  if (_importType === 'olddata') {
    if (!allowedJson.includes(ext)) {
      toast('❌ Old Data Backup only accepts .json files. Please select a .json file.', 'err');
      el('import-file-input').value = '';
      return;
    }
  } else {
    if (!allowedSpreadsheet.includes(ext)) {
      toast(`❌ "${file.name}" is not valid. ${_importType === 'medicines' ? 'Medicine' : _importType === 'sales' ? 'Sales History' : 'Credit'} imports only accept .xlsx or .csv files.`, 'err');
      el('import-file-input').value = '';
      return;
    }
  }

  _importFile = file;
  el('import-file-name').textContent = `📄 ${file.name}`;
  el('import-file-name').classList.remove('hidden');
  el('import-confirm-btn').classList.remove('hidden');
}

async function confirmImport() {
  if (!_importType || !_importFile) { toast('Please select a file first', 'err'); return; }

  // Switch to progress screen
  el('import-step1').classList.add('hidden');
  el('import-step2').classList.remove('hidden');
  el('import-spinner').classList.remove('hidden');
  el('import-done-btn').classList.add('hidden');
  el('import-status-msg').textContent = 'Reading file…';
  el('import-detail-msg').textContent  = '';

  const ext = _importFile.name.split('.').pop().toLowerCase();

  try {
    if (_importType === 'olddata') {
      await _doOldDataImport();
    } else if (ext === 'json') {
      await _doOldDataImport();  // fallback: treat extra json as old data
    } else if (ext === 'csv') {
      await _doSpreadsheetImport('csv');
    } else {
      await _doSpreadsheetImport('xlsx');
    }
  } catch(err) {
    el('import-spinner').classList.add('hidden');
    el('import-status-msg').textContent = '❌ Import failed';
    el('import-detail-msg').textContent = err.message || 'Unknown error';
    el('import-done-btn').classList.remove('hidden');
  }
}

// ── OLD DATA (.json) import ───────────────────────────────
async function _doOldDataImport() {
  el('import-status-msg').textContent = 'Reading backup file…';
  const text = await _readFileAsText(_importFile);
  let parsed;
  try { parsed = JSON.parse(text); } catch { throw new Error('Invalid JSON file — could not parse.'); }
  if (!parsed.products && !parsed.bills && !parsed.settings) {
    throw new Error('This does not look like a PharmaCare backup file (missing expected fields).');
  }

  el('import-status-msg').textContent = 'Creating backup of current data…';
  // Auto-backup current data before overwriting
  if (IS_FLASK) {
    try {
      const bkResp = await fetch('/api/export/backup');
      if (bkResp.ok) {
        const bkData = await bkResp.json();
        const bkBlob = new Blob([JSON.stringify(bkData, null, 2)], {type:'application/json'});
        const bkUrl  = URL.createObjectURL(bkBlob);
        const a = document.createElement('a');
        a.href = bkUrl;
        a.download = `pharmacare_auto_backup_${today()}.json`;
        a.click();
        URL.revokeObjectURL(bkUrl);
      }
    } catch(_){}
  }

  el('import-status-msg').textContent = 'Importing data…';

  if (IS_FLASK) {
    const resp = await fetch('/api/import', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(parsed),
    });
    if (!resp.ok) throw new Error(`Server error ${resp.status}`);
    const result = await resp.json();
    // Reload state from server
    await loadState();
    applyBranding(); renderDashboard();
    el('import-spinner').classList.add('hidden');
    el('import-status-msg').textContent = '✅ Old data restored successfully!';
    el('import-detail-msg').textContent =
      `Products: ${result.counts?.products ?? '?'} · Bills: ${result.counts?.bills ?? '?'} · Credits: ${result.counts?.credits ?? '?'}. A backup of your previous data was automatically downloaded.`;
  } else {
    STATE = { ...STATE, ...parsed };
    saveState(); applyBranding(); renderDashboard();
    el('import-spinner').classList.add('hidden');
    el('import-status-msg').textContent = '✅ Old data restored successfully!';
    el('import-detail-msg').textContent = 'All records have been restored from the backup.';
  }
  el('import-done-btn').classList.remove('hidden');
}

// ── SPREADSHEET (CSV / XLSX) import ──────────────────────
async function _doSpreadsheetImport(fileType) {
  el('import-status-msg').textContent = `Parsing ${fileType.toUpperCase()} file…`;

  let rows = [];
  if (fileType === 'csv') {
    const text = await _readFileAsText(_importFile);
    rows = _parseCSV(text);
  } else {
    rows = await _parseXLSX(_importFile);
  }

  if (!rows || rows.length === 0) throw new Error('No data rows found in the uploaded file.');

  el('import-status-msg').textContent = 'Sending to server…';

  let endpoint, bodyKey;
  if (_importType === 'medicines')    { endpoint = '/api/import/medicines';    bodyKey = 'medicines'; }
  else if (_importType === 'sales')   { endpoint = '/api/import/sales-history'; bodyKey = 'bills'; }
  else if (_importType === 'credits') { endpoint = '/api/import/credits';       bodyKey = 'credits'; }
  else throw new Error('Unknown import type.');

  const resp = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ [bodyKey]: rows }),
  });
  if (!resp.ok) {
    const err = await resp.json().catch(()=>({}));
    throw new Error(err.error || `Server error ${resp.status}`);
  }
  const result = await resp.json();

  // Reload state so new medicines/bills appear immediately
  await loadState();
  applyBranding(); renderDashboard();

  el('import-spinner').classList.add('hidden');
  el('import-status-msg').textContent = '✅ Import completed!';
  const storeLabel = (STATE.settings.storeType || 'your pharmacy').trim();
  el('import-detail-msg').innerHTML =
    `Saved to <strong>${storeLabel}</strong> · ` +
    `Inserted: <strong>${result.inserted ?? 0}</strong> · ` +
    `Updated: <strong>${result.updated ?? 0}</strong> · ` +
    `Skipped: <strong>${result.skipped ?? 0}</strong>` +
    (result.errors?.length ? `<br><span class="text-amber-600 text-xs">⚠ ${result.errors.length} row(s) had errors</span>` : '');
  el('import-done-btn').classList.remove('hidden');
}

// ── CSV parser (RFC 4180 compliant) ──────────────────────
function _parseCSV(text) {
  const lines = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];
  const headers = _splitCSVLine(lines[0]);
  const result  = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = _splitCSVLine(lines[i]);
    if (vals.every(v => !v.trim())) continue;
    const obj = {};
    headers.forEach((h, idx) => { obj[h.trim()] = (vals[idx] || '').trim(); });
    result.push(obj);
  }
  return result;
}
function _splitCSVLine(line) {
  const result = []; let cur = ''; let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') { inQ = !inQ; continue; }
    if (ch === ',' && !inQ) { result.push(cur); cur = ''; continue; }
    cur += ch;
  }
  result.push(cur);
  return result;
}

// ── XLSX parser using SheetJS (CDN) ──────────────────────
function _parseXLSX(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        if (typeof XLSX === 'undefined') {
          // Dynamically load SheetJS if not present
          const s = document.createElement('script');
          s.src = 'https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js';
          s.onload = () => { resolve(_xlsxBufToRows(e.target.result)); };
          s.onerror = () => reject(new Error('Could not load XLSX parser. Check internet connection.'));
          document.head.appendChild(s);
        } else {
          resolve(_xlsxBufToRows(e.target.result));
        }
      } catch(err) { reject(err); }
    };
    reader.onerror = () => reject(new Error('Failed to read file'));
    reader.readAsArrayBuffer(file);
  });
}
function _xlsxBufToRows(buf) {
  const wb      = XLSX.read(buf, { type: 'array' });
  const ws      = wb.Sheets[wb.SheetNames[0]];
  const jsonArr = XLSX.utils.sheet_to_json(ws, { defval: '' });
  // Strip "Column1." / "Column2." style prefixes that some Excel exporters add
  return jsonArr.map(row => {
    const clean = {};
    for (const [k, v] of Object.entries(row)) {
      clean[k.replace(/^Column\d+\./i, '').trim()] = v;
    }
    return clean;
  });
}

// ── File reader helper ────────────────────────────────────
function _readFileAsText(file) {
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload  = e => resolve(e.target.result);
    r.onerror = () => reject(new Error('Failed to read file'));
    r.readAsText(file);
  });
}

// ══════════════════════════════════════════════════════════
//  EXPORT functions
// ══════════════════════════════════════════════════════════
function exportMedicinesCSV() {
  const prods = STATE.products || [];
  const storeType = (STATE.settings.storeType || '').trim();
  const isWS = storeType === 'Wholesale Pharma';
  const filtered = prods.filter(p => isWS
    ? (p.partition === 'wholesale' || p.partition === 'both')
    : (p.partition === 'retail'    || p.partition === 'both')
  );
  const headers = ['id','name','category','unit','purchase','sale','gst','stock','minStock','sku','expiry','brand','hsn','desc'];
  const rows = filtered.map(p => headers.map(h => {
    const v = p[h] ?? '';
    return `"${String(v).replace(/"/g,'""')}"`;
  }).join(','));
  const csv = [headers.join(','), ...rows].join('\n');
  _downloadText(csv, `medicines_${today()}.csv`, 'text/csv');
  closeExportModal();
  toast('Medicine inventory exported ✓');
}

function exportSalesCSV() {
  const bills = STATE.bills || [];
  const storeType = (STATE.settings.storeType || '').trim();
  const isWS = storeType === 'Wholesale Pharma';
  const filtered = bills.filter(b => isWS ? b.billStoreType === 'wholesale' : b.billStoreType !== 'wholesale');
  const headers = ['id','billNo','date','customer','phone','doctor','paymentMode','subtotal','totalDiscount','totalGst','grandTotal'];
  const rows = filtered.map(b => headers.map(h => {
    const v = b[h] ?? '';
    return `"${String(v).replace(/"/g,'""')}"`;
  }).join(','));
  const csv = [headers.join(','), ...rows].join('\n');
  _downloadText(csv, `sales_history_${today()}.csv`, 'text/csv');
  closeExportModal();
  toast('Sales history exported ✓');
}

function exportCreditsCSV() {
  const storeType = (STATE.settings.storeType || '').trim();
  const isWS = storeType === 'Wholesale Pharma';
  let headers, rows;
  if (isWS) {
    const credits = STATE.credits || [];
    headers = ['id','date','shopName','shopkeeperName','phone','forItem','amount','method','status'];
    rows = credits.map(c => headers.map(h => `"${String(c[h]??'').replace(/"/g,'""')}"`).join(','));
  } else {
    const sc = STATE.shopCredits || [];
    headers = ['id','supplierId','supplierName','ownerName','totalPurchase','paid','paymentMode','pending','lastPurchaseDate','status'];
    rows = sc.map(s => headers.map(h => `"${String(s[h]??'').replace(/"/g,'""')}"`).join(','));
  }
  const csv = [headers.join(','), ...rows].join('\n');
  _downloadText(csv, `credits_${today()}.csv`, 'text/csv');
  closeExportModal();
  toast('Credit records exported ✓');
}

async function exportFullBackup() {
  try {
    if (IS_FLASK) {
      const resp = await fetch('/api/export/backup');
      if (!resp.ok) throw new Error('Server error');
      const data = await resp.json();
      _downloadText(JSON.stringify(data, null, 2), `pharmacare_backup_${today()}.json`, 'application/json');
    } else {
      _downloadText(JSON.stringify(STATE, null, 2), `pharmacare_backup_${today()}.json`, 'application/json');
    }
    closeExportModal();
    toast('Full backup exported ✓');
  } catch(err) {
    toast('Export failed: ' + err.message, 'err');
  }
}

// Legacy wrapper (used by old code paths if any)
function exportData() { exportFullBackup(); }

function _downloadText(text, filename, mime) {
  const blob = new Blob([text], { type: mime });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

// Legacy importData kept for backwards compat (hidden file input)
function importData(event) {
  const file = event.target.files[0]; if (!file) return;
  const reader = new FileReader();
  reader.onload = e => {
    try {
      const parsed = JSON.parse(e.target.result);
      if (!parsed.products && !parsed.bills) { toast('Invalid backup file', 'err'); return; }
      if (!confirm('Replace ALL current data with this backup?')) return;
      if (IS_FLASK) {
        fetch('/api/import', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(parsed),
        }).then(() => { loadState().then(() => { applyBranding(); renderDashboard(); toast('Data imported ✓'); }); })
          .catch(() => toast('Import failed', 'err'));
      } else {
        STATE = { ...STATE, ...parsed }; saveState(); applyBranding(); renderDashboard(); toast('Data imported ✓');
      }
    } catch { toast('Error reading file', 'err'); }
  };
  reader.readAsText(file); event.target.value = '';
}

// Toggle conditional settings fields based on pharmacy type
function togglePharmacyTypeFields() {
  const type = (document.getElementById('set-type')?.value || STATE.settings.storeType || '').trim();
  const wFields = document.getElementById('wholesale-fields');
  const rFields = document.getElementById('retail-fields');
  const isWholesale = type === 'Wholesale Pharma';
  const isRetail = ['Retail Pharmacy','Hospital Pharmacy','Medical Store','Ayurvedic Store'].includes(type);
  if (wFields) wFields.classList.toggle('hidden', !isWholesale);
  if (rFields) rFields.classList.toggle('hidden', !isRetail);
  // Show only the relevant QR upload block
  const wsQrBlock = document.getElementById('qr-wholesale-block');
  const rtQrBlock = document.getElementById('qr-retail-block');
  if (wsQrBlock) wsQrBlock.classList.toggle('hidden', !isWholesale);
  if (rtQrBlock) rtQrBlock.classList.toggle('hidden', !isRetail);
}

// Send thank-you notification when a new email is registered (no mailto popup)
function sendThankYouEmail(email) {
  toast('Settings saved! Alerts will be sent to: ' + email);
}

// Alert notification (no mailto popup to avoid OS file-handler dialogs)
function sendAlertEmail(subject, bodyText) {
  // Just log internally — no window.open(mailto) to prevent browser xdg-open popups
  console.info('[PharmaCare Alert]', subject);
}
function resetAllData() { if (!confirm('⚠️ RESET ALL DATA?')) return; if (!confirm('This cannot be undone!')) return; localStorage.removeItem('pharmacare_v2'); location.reload(); }

// ══════════════════════════════════════════════════
// CREDIT SECTION — Wholesale Pharma only
// ══════════════════════════════════════════════════
let creditFilter = 'all';

function checkPharmacyTypeCredit() {
  const type = (document.getElementById('set-type')?.value || STATE.settings.storeType || '').trim();
  const navLink = document.getElementById('nav-credit-link');
  if (!navLink) return;
  const isWholesale = type === 'Wholesale Pharma';
  const isRetail    = ['Retail Pharmacy','Hospital Pharmacy','Medical Store','Ayurvedic Store'].includes(type);

  if (isWholesale || isRetail) {
    navLink.classList.remove('hidden');
  } else {
    navLink.classList.add('hidden');
    const activePage = document.querySelector('.nav-link.active')?.dataset.page;
    if (activePage === 'credit') navigate('dashboard');
    return;
  }

  // Switch views
  const wView = document.getElementById('wholesale-credit-view');
  const rView = document.getElementById('retail-credit-view');
  if (wView) wView.classList.toggle('hidden', !isWholesale);
  if (rView) rView.classList.toggle('hidden', !isRetail);

  if (isWholesale && !STATE.credits) STATE.credits = [];
  if (isRetail && !STATE.shopCredits) STATE.shopCredits = [];

  // Switch Sales History table headers
  const histRetail    = document.getElementById('history-table-retail');
  const histWholesale = document.getElementById('history-table-wholesale');
  if (histRetail)    histRetail.classList.toggle('hidden', isWholesale);
  if (histWholesale) histWholesale.classList.toggle('hidden', !isWholesale);
  renderHistory();
}

function toggleCreditForm() {
  const wrap = document.getElementById('credit-form-wrap');
  if (!wrap) return;
  wrap.classList.toggle('hidden');
  if (!wrap.classList.contains('hidden')) {
    document.getElementById('cr-date').value = today();
  }
}

function addCreditEntry() {
  const date            = document.getElementById('cr-date')?.value;
  const shopName        = document.getElementById('cr-shop')?.value.trim();
  const shopkeeperName  = document.getElementById('cr-name')?.value.trim();
  const phone           = document.getElementById('cr-phone')?.value.trim();
  const forItem         = document.getElementById('cr-item')?.value.trim();
  const amount          = parseFloat(document.getElementById('cr-amount')?.value);
  const method          = document.getElementById('cr-method')?.value;
  const status          = document.getElementById('cr-status')?.value;
  if (!date || !shopName || !shopkeeperName || isNaN(amount) || amount <= 0) {
    toast('Date, Shop Name, Shopkeeper Name and Amount are required', 'err'); return;
  }
  if (!STATE.credits) STATE.credits = [];
  STATE.credits.unshift({ id:uid(), date, shopName, shopkeeperName, phone, forItem, amount, method, status });
  saveState();
  ['cr-shop','cr-name','cr-phone','cr-item','cr-amount'].forEach(id => { const el=document.getElementById(id); if(el) el.value=''; });
  document.getElementById('cr-method').value = 'UPI';
  document.getElementById('cr-status').value = 'Pending';
  toggleCreditForm();
  renderCreditTable(creditFilter);
  toast('Payment receipt added ✓');
}

function renderCreditTable(filter) {
  creditFilter = filter || 'all';
  const tbody = document.getElementById('credit-tbody'); if (!tbody) return;

  // Highlight active filter button
  ['all','7','30','90'].forEach(f => {
    const btn = document.getElementById('cf-' + f);
    if (!btn) return;
    const active = f === creditFilter;
    btn.style.fontWeight   = active ? '700' : '400';
    btn.style.color        = active ? 'var(--accent)' : '';
    btn.style.borderColor  = active ? 'var(--accent)' : '';
    btn.style.background   = active ? '#f0f9ff' : '';
  });

  if (!STATE.credits || STATE.credits.length === 0) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="10" style="text-align:center;padding:28px;color:#94a3b8;font-style:italic">No payment records yet. Click "+ Add a Payment Receipt" to begin.</td></tr>';
    updateCreditSummary([]);
    return;
  }

  const now = new Date();
  let filtered = [...STATE.credits];
  if (filter === '7')  { const c=new Date(); c.setDate(now.getDate()-7);                       filtered=filtered.filter(e=>new Date(e.date)>=c); }
  if (filter === '30') { const c=new Date(now.getFullYear(),now.getMonth(),1);                  filtered=filtered.filter(e=>new Date(e.date)>=c); }
  if (filter === '90') { const c=new Date(); c.setDate(now.getDate()-90);                      filtered=filtered.filter(e=>new Date(e.date)>=c); }

  updateCreditSummary(filtered);

  if (filtered.length === 0) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="10" style="text-align:center;padding:28px;color:#94a3b8;font-style:italic">No records in this period.</td></tr>'; return;
  }

  tbody.innerHTML = filtered.map((c,i) => {
    const isPending = c.status === 'Pending';
    const statusBadge = isPending
      ? '<span class="badge badge-red" style="font-weight:700">Pending</span>'
      : '<span class="badge badge-green" style="font-weight:700">Cleared</span>';
    const actionBtn = isPending
      ? `<button onclick="markCreditCleared('${c.id}')" style="background:#ecfdf5;color:#059669;padding:4px 10px;border-radius:6px;font-size:11px;font-weight:700;border:1px solid #a7f3d0;cursor:pointer;white-space:nowrap">✓ Received</button>`
      : '<span style="color:#94a3b8;font-size:11px;font-style:italic">Done</span>';
    return `<tr style="${isPending ? 'background:#fffbeb' : ''}">
      <td style="color:#94a3b8;font-size:12px;text-align:center">${i+1}</td>
      <td style="font-size:12px;white-space:nowrap">${fmtDate(c.date)}</td>
      <td style="font-weight:600">${c.shopName}</td>
      <td>${c.shopkeeperName}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-size:12px">${c.phone||'—'}</td>
      <td style="font-size:12px">${c.forItem||'—'}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-weight:700;color:var(--accent)">${cur(c.amount)}</td>
      <td><span class="badge badge-blue">${c.method}</span></td>
      <td>${statusBadge}</td>
      <td>${actionBtn}</td>
    </tr>`;
  }).join('');
}

function updateCreditSummary(filtered) {
  const totalAmt   = filtered.reduce((s,c)=>s+c.amount,0);
  const pendingAmt = filtered.filter(c=>c.status==='Pending').reduce((s,c)=>s+c.amount,0);
  const clearedAmt = filtered.filter(c=>c.status==='Cleared').reduce((s,c)=>s+c.amount,0);
  const el = document.getElementById('credit-summary-bar');
  if (!el) return;
  el.innerHTML = `
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:10px;padding:10px 16px;text-align:center">
      <div style="font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase">Total</div>
      <div style="font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;color:#0ea5e9">${cur(totalAmt)}</div>
    </div>
    <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:10px;padding:10px 16px;text-align:center">
      <div style="font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase">Pending</div>
      <div style="font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;color:#ef4444">${cur(pendingAmt)}</div>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:10px 16px;text-align:center">
      <div style="font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase">Cleared</div>
      <div style="font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;color:#10b981">${cur(clearedAmt)}</div>
    </div>`;
}

function markCreditCleared(id) {
  const idx = STATE.credits.findIndex(c=>c.id===id); if (idx<0) return;
  const entry = STATE.credits[idx];
  STATE.credits[idx].status = 'Cleared';
  saveState();
  renderCreditTable(creditFilter);

  // Popup
  const msgEl = document.getElementById('payment-received-msg');
  if (msgEl) msgEl.innerHTML = `Payment of <strong>${cur(entry.amount)}</strong> from <strong>${entry.shopkeeperName}</strong><br><span style="color:#64748b;font-size:12px">${entry.shopName} · ${entry.method}</span>`;
  openModal('payment-received-modal');

  // Email alert to registered pharmacy email
  const email = STATE.settings.email || '';
  if (email) {
    const alertSubject = `Payment Received: ${entry.shopkeeperName} — ${cur(entry.amount)}`;
    const alertBody    = `Hello,\n\nA payment has been received and cleared.\n\nShopkeeper : ${entry.shopkeeperName}\nShop Name  : ${entry.shopName}\nPhone      : ${entry.phone||'—'}\nAmount     : ${cur(entry.amount)}\nMethod     : ${entry.method}\nItem       : ${entry.forItem||'—'}\nDate       : ${fmtDate(entry.date)}\n\nThis record has been marked as Cleared in PharmaCare Pro.\n\nRegards,\n${STATE.settings.storeName}`;
    sendAlertEmail(alertSubject, alertBody);
  }
  toast(`${entry.shopkeeperName}'s payment marked as Cleared ✓`);
}

function openClearCreditModal() { openModal('clear-credit-modal'); }

function clearCreditEntries(period) {
  const label = period==='7' ? 'last 7 days' : period==='30' ? 'this month' : 'last 90 days';
  if (!confirm(`Permanently delete all credit records from ${label}? This cannot be undone.`)) return;
  const now = new Date();
  let cutoff;
  if (period==='7')  { cutoff=new Date(); cutoff.setDate(now.getDate()-7); }
  if (period==='30') { cutoff=new Date(now.getFullYear(),now.getMonth(),1); }
  if (period==='90') { cutoff=new Date(); cutoff.setDate(now.getDate()-90); }
  STATE.credits = (STATE.credits||[]).filter(c => new Date(c.date) < cutoff);
  saveState();
  closeModal('clear-credit-modal');
  renderCreditTable(creditFilter);
  toast(`Records from ${label} cleared ✓`);
}

// ══════════════════════════════════════════════════
// RETAIL CREDIT SYSTEM
// (Retail Pharmacy, Hospital Pharmacy, Medical Store, Ayurvedic Store)
// ══════════════════════════════════════════════════

let retailSelectMode = false;

function toggleRetailSelectMode() {
  retailSelectMode = !retailSelectMode;
  const col = document.getElementById('retail-select-col');
  const deleteBtn = document.getElementById('retail-delete-btn');
  const allChk = document.getElementById('retail-select-all');
  if (col) col.classList.toggle('hidden', !retailSelectMode);
  if (deleteBtn) deleteBtn.classList.toggle('hidden', !retailSelectMode);
  if (allChk) allChk.checked = false;
  if (!retailSelectMode) document.querySelectorAll('.retail-row-chk').forEach(c => c.checked = false);
  renderRetailCreditTable();
}

function selectAllRetailCredits(checked) {
  document.querySelectorAll('.retail-row-chk').forEach(c => c.checked = checked);
}

function deleteSelectedRetailCredits() {
  const selected = [...document.querySelectorAll('.retail-row-chk:checked')].map(c => c.dataset.id);
  if (selected.length === 0) { toast('No records selected', 'err'); return; }
  const msgEl = document.getElementById('delete-retail-credit-msg');
  if (msgEl) msgEl.textContent = `Are you sure you want to delete ${selected.length} selected record${selected.length > 1 ? 's' : ''}? This cannot be undone.`;
  window._pendingDeleteRetailIds = selected;
  openModal('delete-retail-credit-modal');
}

function confirmDeleteRetailCredits() {
  const ids = window._pendingDeleteRetailIds || [];
  STATE.shopCredits = (STATE.shopCredits || []).filter(r => !ids.includes(r.id));
  saveState();
  closeModal('delete-retail-credit-modal');
  retailSelectMode = false;
  const col = document.getElementById('retail-select-col');
  const deleteBtn = document.getElementById('retail-delete-btn');
  if (col) col.classList.add('hidden');
  if (deleteBtn) deleteBtn.classList.add('hidden');
  renderRetailCreditTable();
  toast(`${ids.length} record${ids.length > 1 ? 's' : ''} deleted ✓`);
}

function renderRetailCreditTable() {
  const tbody = document.getElementById('retail-credit-tbody'); if (!tbody) return;
  if (!STATE.shopCredits || STATE.shopCredits.length === 0) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="11" style="text-align:center;padding:28px;color:#94a3b8;font-style:italic">No supplier records yet. Click "💳 Pay Now" to add.</td></tr>';
    updateRetailCreditSummary([]);
    return;
  }
  updateRetailCreditSummary(STATE.shopCredits);
  tbody.innerHTML = STATE.shopCredits.map((r,i) => {
    const isPending = r.status === 'Pending';
    const statusDot = isPending
      ? `<span style="display:inline-flex;align-items:center;gap:5px;font-weight:700;color:#ef4444"><span style="width:10px;height:10px;border-radius:50%;background:#ef4444;display:inline-block"></span>Pending</span>`
      : `<span style="display:inline-flex;align-items:center;gap:5px;font-weight:700;color:#10b981"><span style="width:10px;height:10px;border-radius:50%;background:#10b981;display:inline-block"></span>Cleared</span>`;
    const checkboxCell = retailSelectMode
      ? `<td style="text-align:center"><input type="checkbox" class="retail-row-chk" data-id="${r.id}" style="cursor:pointer;width:15px;height:15px"/></td>`
      : '';
    return `<tr style="${isPending ? 'background:#fffbeb' : ''}">
      ${checkboxCell}
      <td style="color:#94a3b8;font-size:12px;text-align:center">${i+1}</td>
      <td style="font-weight:600">${r.supplierName}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#0ea5e9;font-weight:700">${r.supplierId}</td>
      <td>${r.ownerName}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-weight:700">${cur(r.totalPurchase)}</td>
      <td style="font-family:'JetBrains Mono',monospace;color:#10b981;font-weight:700">${cur(r.paid)}</td>
      <td><span class="badge badge-blue">${r.paymentMode}</span></td>
      <td style="font-family:'JetBrains Mono',monospace;font-weight:700;color:${r.pending>0?'#ef4444':'#10b981'}">${cur(r.pending)}</td>
      <td style="font-size:12px;white-space:nowrap">${fmtDate(r.lastPurchaseDate)}</td>
      <td>${statusDot}</td>
    </tr>`;
  }).join('');
}

function updateRetailCreditSummary(records) {
  const el = document.getElementById('retail-credit-summary-bar'); if (!el) return;

  // Only count the LATEST record per supplier (first occurrence = most recent, since we unshift)
  const seen = new Set();
  const latestPerSupplier = records.filter(r => {
    const key = r.supplierId.toLowerCase() + '|' + r.ownerName.toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  const totalPurchase = latestPerSupplier.reduce((s,r)=>s+r.totalPurchase, 0);
  const totalPaid     = latestPerSupplier.reduce((s,r)=>s+r.paid, 0);
  const totalPending  = latestPerSupplier.reduce((s,r)=>s+r.pending, 0);
  el.innerHTML = `
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:10px;padding:10px 16px;text-align:center;flex:1;min-width:120px">
      <div style="font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase">Total Purchase</div>
      <div style="font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;color:#0ea5e9">${cur(totalPurchase)}</div>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:10px 16px;text-align:center;flex:1;min-width:120px">
      <div style="font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase">Total Paid</div>
      <div style="font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;color:#10b981">${cur(totalPaid)}</div>
    </div>
    <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:10px;padding:10px 16px;text-align:center;flex:1;min-width:120px">
      <div style="font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase">Total Pending</div>
      <div style="font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;color:#ef4444">${cur(totalPending)}</div>
    </div>`;
}

// Open Pay Now modal
function openPayNowModal() {
  clearPayNowForm();
  document.getElementById('sc-bill-date').value = today();
  openModal('pay-now-modal');
}

// Clear all form fields + unlock
function clearPayNowForm() {
  ['sc-supplier','sc-wid','sc-owner','sc-total-purchase','sc-paid','sc-last-date','sc-bill-date'].forEach(id => {
    const el = document.getElementById(id);
    if (el) { el.value = ''; el.readOnly = false; el.style.background = ''; el.style.cursor = ''; el.style.border = ''; }
  });
  // Pending is always auto-calculated — keep readonly
  const pendingEl = document.getElementById('sc-pending');
  if (pendingEl) { pendingEl.value = ''; pendingEl.readOnly = true; pendingEl.style.background = '#f1f5f9'; pendingEl.style.cursor = 'not-allowed'; }
  // Status is always auto-set — keep disabled
  const statusEl = document.getElementById('sc-status');
  if (statusEl) { statusEl.value = 'Pending'; statusEl.disabled = true; statusEl.style.background = '#f1f5f9'; statusEl.style.cursor = 'not-allowed'; }
  document.getElementById('sc-method').value = 'UPI';
  document.getElementById('sc-method').disabled = false;
  document.getElementById('sc-bill-date').value = today();
  // Hide EDIT FETCHED button and info bar
  const editBtn = document.getElementById('edit-fetched-btn');
  if (editBtn) editBtn.classList.add('hidden');
  const infoBar = document.getElementById('fetch-info-bar');
  if (infoBar) infoBar.classList.add('hidden');
  window._fetchLocked = false;
  window._fetchedRecord = null;
  window._oldPendingForCalc = 0;
}

// Live auto-calculate: Pending = OldPending - NowPaying (if fetch used), else TotalPurchase - NowPaying
function autoCalcPending() {
  const totalPurchase = parseFloat(document.getElementById('sc-total-purchase')?.value) || 0;
  const nowPaying     = parseFloat(document.getElementById('sc-paid')?.value) || 0;
  const oldPending    = window._oldPendingForCalc !== undefined ? window._oldPendingForCalc : totalPurchase;
  // If fetch was used: pending = oldPending - nowPaying
  // If manual entry: pending = totalPurchase - nowPaying
  const base = window._fetchedRecord ? oldPending : totalPurchase;
  const pending = Math.max(0, base - nowPaying);
  const pendingEl = document.getElementById('sc-pending');
  const statusEl  = document.getElementById('sc-status');
  if (pendingEl) pendingEl.value = pending.toFixed(2);
  if (statusEl)  statusEl.value  = pending <= 0 ? 'Cleared' : 'Pending';
}

// FETCH DETAILS — last-in-first-show by Wholeseller ID + Owner Name
function fetchSupplierDetails() {
  const wid   = document.getElementById('sc-wid')?.value.trim();
  const owner = document.getElementById('sc-owner')?.value.trim();
  if (!wid || !owner) { toast('Enter Wholeseller ID and Owner Name first', 'err'); return; }
  if (!STATE.shopCredits) { toast('No records found', 'err'); return; }
  // Find latest record matching ID + owner (first in array = last added)
  const match = STATE.shopCredits.find(r =>
    r.supplierId.toLowerCase() === wid.toLowerCase() &&
    r.ownerName.toLowerCase() === owner.toLowerCase()
  );
  if (!match) { toast('No existing record found for this Wholeseller ID & Owner Name', 'err'); return; }

  // Show previous values info bar
  const infoBar = document.getElementById('fetch-info-bar');
  if (infoBar) {
    infoBar.classList.remove('hidden');
    const cur2 = v => (STATE.settings.currency||'₹') + parseFloat(v||0).toFixed(2);
    document.getElementById('fi-total').textContent   = cur2(match.totalPurchase);
    document.getElementById('fi-paid').textContent    = cur2(match.paid);
    document.getElementById('fi-pending').textContent = cur2(match.pending);
    document.getElementById('fi-status').textContent  = match.status;
    document.getElementById('fi-status').style.color  = match.status === 'Cleared' ? '#10b981' : '#ef4444';
  }

  // Fill fields as read-only (locked)
  const setLocked = (id, val) => {
    const el = document.getElementById(id);
    if (el) { el.value = val || ''; el.readOnly = true; el.style.background = '#f1f5f9'; el.style.cursor = 'not-allowed'; }
  };
  setLocked('sc-supplier',       match.supplierName);
  setLocked('sc-total-purchase', match.totalPurchase);
  setLocked('sc-paid',           match.paid);
  setLocked('sc-pending',        match.pending);
  setLocked('sc-last-date',      match.lastPurchaseDate);
  setLocked('sc-bill-date',      today());
  const mEl = document.getElementById('sc-method'); if (mEl) { mEl.value = match.paymentMode; mEl.disabled = true; }
  const sEl = document.getElementById('sc-status'); if (sEl) { sEl.value = match.status; sEl.disabled = true; }

  // Store old pending for live calc
  window._oldPendingForCalc = match.pending;
  window._fetchedRecord = match;
  window._fetchLocked = true;

  // Show EDIT FETCHED button
  const editBtn = document.getElementById('edit-fetched-btn');
  if (editBtn) editBtn.classList.remove('hidden');

  toast(`Fetched! Old Pending: ₹${match.pending}. Click ✏️ EDIT FETCHED → enter amount paying now → Pending auto-updates`);
}

// EDIT FETCHED — unlock Total Purchase and Now Paying for editing; Pending stays auto-calculated
function enableEditFetched() {
  // Unlock Total Purchase (in case new goods purchased)
  const totalEl = document.getElementById('sc-total-purchase');
  if (totalEl) { totalEl.readOnly = false; totalEl.style.background = '#fffbeb'; totalEl.style.cursor = 'text'; totalEl.style.border = '2px solid #f59e0b'; }
  // Unlock Now Paying — reset to 0 so user types the new payment
  const paidEl = document.getElementById('sc-paid');
  if (paidEl) { paidEl.readOnly = false; paidEl.value = ''; paidEl.style.background = '#fffbeb'; paidEl.style.cursor = 'text'; paidEl.style.border = '2px solid #10b981'; paidEl.placeholder = 'Enter amount paying now'; }
  // Unlock Payment Mode
  const mEl = document.getElementById('sc-method'); if (mEl) mEl.disabled = false;
  // Pending stays readonly — auto-calculated via autoCalcPending()
  // Reset pending display to old pending
  const pendingEl = document.getElementById('sc-pending');
  if (pendingEl) pendingEl.value = (window._oldPendingForCalc || 0).toFixed(2);
  const statusEl = document.getElementById('sc-status');
  if (statusEl) statusEl.value = 'Pending';
  // Hide EDIT FETCHED button
  const editBtn = document.getElementById('edit-fetched-btn');
  if (editBtn) editBtn.classList.add('hidden');
  // Focus paid field
  paidEl?.focus();
  toast(`Enter the amount you are paying NOW — Pending will auto-update ✓`);
}

// ADD RECORD
function addRetailCreditRecord() {
  const supplierId       = document.getElementById('sc-wid')?.value.trim();
  const supplierName     = document.getElementById('sc-supplier')?.value.trim();
  const ownerName        = document.getElementById('sc-owner')?.value.trim();
  const totalPurchase    = parseFloat(document.getElementById('sc-total-purchase')?.value) || 0;
  const nowPaying        = parseFloat(document.getElementById('sc-paid')?.value) || 0;
  const paymentMode      = document.getElementById('sc-method')?.value;
  const lastPurchaseDate = document.getElementById('sc-last-date')?.value;
  const billDate         = document.getElementById('sc-bill-date')?.value || today();

  if (!supplierId || !supplierName || !ownerName) {
    toast('Supplier Name, Wholeseller ID and Owner Name are required', 'err'); return;
  }
  if (!STATE.shopCredits) STATE.shopCredits = [];

  let newPending, cumulativePaid;

  if (window._fetchedRecord) {
    // FETCH mode: user entered NEW payment amount in "Now Paying"
    // NEW PENDING = OLD PENDING − NOW PAYING
    const oldPending = window._fetchedRecord.pending;
    const oldPaid    = window._fetchedRecord.paid;
    newPending     = +Math.max(0, oldPending - nowPaying).toFixed(2);
    cumulativePaid = +(oldPaid + nowPaying).toFixed(2);
  } else {
    // MANUAL mode (no fetch): user enters total purchase and payment this time
    // PENDING = TOTAL PURCHASE − NOW PAYING
    newPending     = +Math.max(0, totalPurchase - nowPaying).toFixed(2);
    cumulativePaid = +nowPaying.toFixed(2);
  }

  const status = newPending <= 0 ? 'Cleared' : 'Pending';

  const newRecord = {
    id: uid(), supplierId, supplierName, ownerName,
    totalPurchase,
    paid: cumulativePaid,   // stored as cumulative for display
    paymentMode,
    pending: newPending,
    lastPurchaseDate: lastPurchaseDate || today(),
    billDate, status
  };

  STATE.shopCredits.unshift(newRecord);
  saveState();

  // Reset fetch state
  window._fetchLocked = false;
  window._fetchedRecord = null;
  window._oldPendingForCalc = 0;

  closeModal('pay-now-modal');
  renderRetailCreditTable();

  // Ask to print
  document.getElementById('sc-print-supplier-id').value = supplierId;
  openModal('sc-print-confirm-modal');
}

function printSupplierHistory(supplierId) {
  if (!supplierId) {
    supplierId = document.getElementById('sc-print-supplier-id')?.value;
  }
  closeModal('sc-print-confirm-modal');
  if (!supplierId) return;

  const records = (STATE.shopCredits || []).filter(r =>
    r.supplierId.toLowerCase() === supplierId.toLowerCase()
  );
  if (!records.length) { toast('No records found for this Supplier ID', 'err'); return; }

  const s = STATE.settings;
  const type = (s.storeType || '').trim();
  const isRetail = type !== 'Wholesale Pharma';
  // Build type-specific identity line for credit PDF header
  let identityLine = `${type || ''}`;
  if (isRetail) {
    if (s.shopName)      identityLine += ` · Shop: ${s.shopName}`;
    if (s.retailerOwner) identityLine += ` · Owner: ${s.retailerOwner}`;
  }
  const printWin = window.open('', '_blank', 'width=900,height=700');
  printWin.document.write(`<!DOCTYPE html><html><head><title>Supplier Statement — ${supplierId}</title>
  <style>
    body{font-family:Arial,sans-serif;padding:28px;color:#1e293b}
    h2{margin:0 0 4px;color:#0f172a}
    .sub{color:#64748b;font-size:13px;margin-bottom:20px}
    .store{font-size:15px;font-weight:700;color:#0ea5e9;margin-bottom:2px}
    table{width:100%;border-collapse:collapse;margin-top:16px;font-size:12px}
    th{background:#0ea5e9;color:#fff;padding:8px 10px;text-align:left;font-size:11px}
    td{padding:7px 10px;border-bottom:1px solid #e2e8f0}
    tr:nth-child(even){background:#f8fafc}
    .badge-red{background:#fef2f2;color:#ef4444;padding:2px 8px;border-radius:20px;font-weight:700}
    .badge-green{background:#f0fdf4;color:#10b981;padding:2px 8px;border-radius:20px;font-weight:700}
    .dot-red{display:inline-block;width:8px;height:8px;border-radius:50%;background:#ef4444;margin-right:5px}
    .dot-green{display:inline-block;width:8px;height:8px;border-radius:50%;background:#10b981;margin-right:5px}
    .summary{display:flex;gap:16px;margin-bottom:20px}
    .sum-box{border:1px solid #e2e8f0;border-radius:8px;padding:10px 16px;min-width:140px}
    .sum-label{font-size:10px;text-transform:uppercase;color:#94a3b8;font-weight:600}
    .sum-val{font-size:16px;font-weight:700}
    @media print{body{padding:16px}}
  </style></head><body>
  <div class="store">${s.storeName || 'My Pharmacy'}</div>
  <div style="font-size:11px;color:#64748b;margin-bottom:4px">${identityLine}</div>
  <div style="font-size:11px;color:#64748b;margin-bottom:4px">${s.address || ''}</div>
  <div style="font-size:11px;color:#64748b;margin-bottom:4px">${s.phone ? '📞 ' + s.phone : ''} ${s.email ? '| ✉ ' + s.email : ''}</div>
  <div style="font-size:11px;color:#64748b;margin-bottom:14px">${s.gstin ? 'GSTIN: ' + s.gstin : ''} ${s.license ? '| DL No: ' + s.license : ''}</div>
  <h2>Supplier Payment Statement</h2>
  <div class="sub">Wholeseller ID: <strong>${supplierId}</strong> &nbsp;|&nbsp; Supplier: <strong>${records[0].supplierName}</strong> &nbsp;|&nbsp; Owner: <strong>${records[0].ownerName}</strong></div>
  <div class="sub">Printed on: ${new Date().toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'})}</div>
  <div class="summary">
    <div class="sum-box"><div class="sum-label">Total Purchased</div><div class="sum-val" style="color:#0ea5e9">${(STATE.settings.currency||'₹')}${records.reduce((s,r)=>s+r.totalPurchase,0).toFixed(2)}</div></div>
    <div class="sum-box"><div class="sum-label">Total Paid</div><div class="sum-val" style="color:#10b981">${(STATE.settings.currency||'₹')}${records.reduce((s,r)=>s+r.paid,0).toFixed(2)}</div></div>
    <div class="sum-box"><div class="sum-label">Current Pending</div><div class="sum-val" style="color:#ef4444">${(STATE.settings.currency||'₹')}${records[0].pending.toFixed(2)}</div></div>
  </div>
  <table>
    <thead><tr>
      <th>Sl. No.</th><th>Supplier Name</th><th>Wholeseller ID</th><th>Owner Name</th>
      <th>Total Purchase</th><th>Paid</th><th>Payment Mode</th><th>Pending</th>
      <th>Last Purchase Date</th><th>Status</th>
    </tr></thead>
    <tbody>
    ${records.map((r,i)=>`<tr>
      <td>${i+1}</td>
      <td>${r.supplierName}</td>
      <td style="font-weight:700;color:#0ea5e9">${r.supplierId}</td>
      <td>${r.ownerName}</td>
      <td style="font-weight:700">${(STATE.settings.currency||'₹')}${r.totalPurchase.toFixed(2)}</td>
      <td style="color:#10b981;font-weight:700">${(STATE.settings.currency||'₹')}${r.paid.toFixed(2)}</td>
      <td>${r.paymentMode}</td>
      <td style="color:${r.pending>0?'#ef4444':'#10b981'};font-weight:700">${(STATE.settings.currency||'₹')}${r.pending.toFixed(2)}</td>
      <td>${r.lastPurchaseDate||'—'}</td>
      <td>${r.status==='Pending'?'<span class="dot-red"></span><span class="badge-red">Pending</span>':'<span class="dot-green"></span><span class="badge-green">Cleared</span>'}</td>
    </tr>`).join('')}
    </tbody>
  </table>
  <div style="margin-top:28px;font-size:11px;color:#94a3b8;border-top:1px solid #e2e8f0;padding-top:12px">
    Generated by PharmaCare Pro · ${s.storeName} · ${new Date().toLocaleString('en-IN')}
  </div>
  <script>window.onload=()=>{window.print();}<\/script>
  </body></html>`);
  printWin.document.close();
}
