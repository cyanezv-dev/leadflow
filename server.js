const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json({ limit: "10mb" }));

// ─── CONEXIÓN POSTGRESQL ──────────────────────────────────────────
// Railway inyecta DATABASE_URL automáticamente.
// Para desarrollo local: crea un .env con DATABASE_URL=postgresql://user:pass@localhost:5432/leadflow
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production'
    ? { rejectUnauthorized: false }   // Railway / Render requieren SSL
    : false,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

const query = (text, params) => pool.query(text, params);

pool.connect()
  .then(client => { console.log('PostgreSQL conectado'); client.release(); })
  .catch(err => { console.error('Error DB:', err.message); process.exit(1); });

// ─── INICIALIZAR TABLAS ───────────────────────────────────────────
async function initDB() {
  await query(`
    CREATE TABLE IF NOT EXISTS leads (
      id              TEXT PRIMARY KEY,
      name            TEXT NOT NULL,
      company         TEXT,
      email           TEXT,
      phone           TEXT,
      channel         TEXT DEFAULT 'Web',
      status          TEXT DEFAULT 'Nuevo',
      priority        TEXT DEFAULT 'Media',
      estimated_value NUMERIC DEFAULT 0,
      notes           TEXT,
      agent           TEXT,
      created_at      TIMESTAMPTZ DEFAULT NOW(),
      updated_at      TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS activities (
      id         TEXT PRIMARY KEY,
      lead_id    TEXT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
      type       TEXT NOT NULL,
      content    TEXT,
      channel    TEXT,
      direction  TEXT DEFAULT 'inbound',
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS appointments (
      id           TEXT PRIMARY KEY,
      lead_id      TEXT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
      scheduled_at TIMESTAMPTZ NOT NULL,
      channel      TEXT,
      notes        TEXT,
      status       TEXT DEFAULT 'pending',
      created_at   TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS tasks (
      id         TEXT PRIMARY KEY,
      lead_id    TEXT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
      title      TEXT NOT NULL,
      due_date   TIMESTAMPTZ,
      completed  BOOLEAN DEFAULT FALSE,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS notes (
      id         TEXT PRIMARY KEY,
      lead_id    TEXT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
      content    TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_leads_status    ON leads(status);
    CREATE INDEX IF NOT EXISTS idx_leads_channel   ON leads(channel);
    CREATE INDEX IF NOT EXISTS idx_activities_lead ON activities(lead_id);
    CREATE INDEX IF NOT EXISTS idx_tasks_lead      ON tasks(lead_id);
  `);

  const { rows } = await query('SELECT COUNT(*) as count FROM leads');
  if (parseInt(rows[0].count) === 0) await seedData();

  console.log('Tablas listas');
}

// ─── SEED DATA ────────────────────────────────────────────────────
async function seedData() {
  const leads = [
    { name: 'Maria Gonzalez',   company: 'Tech Innovate MX',   email: 'maria@techinnovate.mx',   phone: '+52 55 1234 5678', channel: 'WhatsApp', status: 'Nuevo',       priority: 'Alta',  estimated_value: 15000, notes: 'Interesada en plan enterprise. Contactar antes del viernes.', agent: 'AI Agent - WhatsApp' },
    { name: 'Carlos Hernandez', company: 'Startup Co',          email: 'carlos@startup.co',        phone: '+52 55 2345 6789', channel: 'Facebook', status: 'Contactado',  priority: 'Media', estimated_value: 8500,  notes: 'Solicita demo del producto.',                                agent: 'AI Agent - Facebook' },
    { name: 'Ana Martinez',     company: 'Boutique Digital',    email: 'ana@boutiquedigital.mx',   phone: '+52 55 3456 7890', channel: 'Instagram',status: 'Calificado',  priority: 'Alta',  estimated_value: 22000, notes: 'Lista para propuesta formal.',                               agent: 'AI Agent - Instagram' },
    { name: 'Roberto Lopez',    company: 'Empresa Global SA',   email: 'roberto@empresaglobal.mx', phone: '+52 55 4567 8901', channel: 'Chat',     status: 'Propuesta',   priority: 'Alta',  estimated_value: 45000, notes: 'Revisando propuesta con equipo.',                            agent: 'AI Agent - Chat' },
    { name: 'Laura Sanchez',    company: 'Consultoria Sanchez', email: 'laura@consultoria.mx',     phone: '+52 55 5678 9012', channel: 'Telefono', status: 'Negociacion', priority: 'Media', estimated_value: 32000, notes: 'Negociando precio final.',                                    agent: 'AI Agent - Telefono' },
    { name: 'Diego Ramirez',    company: 'WebFlow MX',          email: 'diego@webflow.mx',         phone: '+52 55 6789 0123', channel: 'Web',      status: 'Nuevo',       priority: 'Baja',  estimated_value: 3500,  notes: 'Lead frio del blog.',                                        agent: 'AI Agent - Web' },
    { name: 'Patricia Flores',  company: 'Marketing 360',       email: 'patricia@mkt360.mx',       phone: '+52 55 7890 1234', channel: 'WhatsApp', status: 'Ganado',      priority: 'Alta',  estimated_value: 28000, notes: 'Cliente cerrado. Onboarding iniciado.',                      agent: 'AI Agent - WhatsApp' },
    { name: 'Fernando Torres',  company: 'Retail MX',           email: 'fernando@retailmx.com',    phone: '+52 55 8901 2345', channel: 'Facebook', status: 'Perdido',     priority: 'Baja',  estimated_value: 5000,  notes: 'Eligio competidor.',                                         agent: 'AI Agent - Facebook' },
    { name: 'Alejandro Ruiz',   company: 'Innova Tech',         email: 'alejandro@innovatech.mx',  phone: '+52 55 9012 3456', channel: 'Telefono', status: 'Contactado',  priority: 'Media', estimated_value: 9500,  notes: 'Segunda llamada pendiente.',                                 agent: 'AI Agent - Telefono' },
    { name: 'Sofia Mendoza',    company: 'FinTech Solutions',   email: 'sofia@fintech.mx',         phone: '+52 55 0123 4567', channel: 'Instagram',status: 'Nuevo',       priority: 'Alta',  estimated_value: 18000, notes: 'Referida por Patricia Flores.',                              agent: 'AI Agent - Instagram' },
  ];

  let firstLeadId = null;
  for (const l of leads) {
    const id = uuidv4();
    if (!firstLeadId) firstLeadId = id;
    await query(
      `INSERT INTO leads (id, name, last_name, company, email, phone, channel, status, priority, estimated_value, notes, agent)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
      [id, l.name, l.company, l.email, l.phone, l.channel, l.status, l.priority, l.estimated_value, l.notes, l.agent]
    );
  }

  await query(
    `INSERT INTO activities (id, lead_id, type, content, channel, direction) VALUES ($1,$2,$3,$4,$5,$6)`,
    [uuidv4(), firstLeadId, 'Message', 'Hola, estoy interesada en conocer mas sobre sus servicios enterprise.', 'WhatsApp', 'inbound']
  );
  await query(
    `INSERT INTO activities (id, lead_id, type, content, channel, direction) VALUES ($1,$2,$3,$4,$5,$6)`,
    [uuidv4(), firstLeadId, 'Message', 'Hola! Con gusto te ayudo. Cuantos usuarios necesitarias?', 'WhatsApp', 'outbound']
  );
  console.log('Datos de ejemplo cargados');
}

// ─── ASYNC HANDLER ────────────────────────────────────────────────
const asyncHandler = fn => (req, res, next) =>
  Promise.resolve(fn(req, res, next)).catch(err => {
    console.error('API Error:', err.message);
    res.status(500).json({ error: 'Error interno del servidor', detail: err.message });
  });

// ─── HEALTH CHECK ─────────────────────────────────────────────────
app.get('/health', asyncHandler(async (req, res) => {
  await query('SELECT 1');
  res.json({ status: 'ok', db: 'connected', timestamp: new Date().toISOString() });
}));

// ─── DASHBOARD ────────────────────────────────────────────────────
app.get('/api/dashboard', asyncHandler(async (req, res) => {
  const [total, nuevos, ganados, valor, byChannel, recientes] = await Promise.all([
    query('SELECT COUNT(*) as count FROM leads'),
    query("SELECT COUNT(*) as count FROM leads WHERE status = 'Nuevo'"),
    query("SELECT COUNT(*) as count FROM leads WHERE status = 'Ganado'"),
    query('SELECT COALESCE(SUM(estimated_value), 0) as total FROM leads'),
    query('SELECT channel, COUNT(*) as count FROM leads GROUP BY channel ORDER BY count DESC'),
    query('SELECT * FROM leads ORDER BY created_at DESC LIMIT 10'),
  ]);

  res.json({
    totalLeads:  parseInt(total.rows[0].count),
    newLeads:    parseInt(nuevos.rows[0].count),
    wonLeads:    parseInt(ganados.rows[0].count),
    totalValue:  parseFloat(valor.rows[0].total),
    byChannel:   byChannel.rows,
    recentLeads: recientes.rows,
  });
}));

// ─── LEADS CRUD ───────────────────────────────────────────────────
app.get('/api/leads', asyncHandler(async (req, res) => {
  const { status, channel, search } = req.query;
  const conditions = ['1=1'];
  const params = [];
  let i = 1;

  if (status)  { conditions.push(`status = $${i++}`);  params.push(status); }
  if (channel) { conditions.push(`channel = $${i++}`); params.push(channel); }
  if (search)  {
    conditions.push(`(name ILIKE $${i} OR company ILIKE $${i} OR email ILIKE $${i})`);
    params.push(`%${search}%`);
    i++;
  }

  const { rows } = await query(
    `SELECT * FROM leads WHERE ${conditions.join(' AND ')} ORDER BY created_at DESC`,
    params
  );
  res.json(rows);
}));

app.get('/api/leads/:id', asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { rows: [lead] } = await query('SELECT * FROM leads WHERE id = $1', [id]);
  if (!lead) return res.status(404).json({ error: 'Lead no encontrado' });

  const [activities, appointments, tasks, notes] = await Promise.all([
    query('SELECT * FROM activities   WHERE lead_id = $1 ORDER BY created_at DESC', [id]),
    query('SELECT * FROM appointments WHERE lead_id = $1 ORDER BY scheduled_at ASC', [id]),
    query('SELECT * FROM tasks        WHERE lead_id = $1 ORDER BY created_at ASC',   [id]),
    query('SELECT * FROM notes        WHERE lead_id = $1 ORDER BY created_at DESC',  [id]),
  ]);

  res.json({
    ...lead,
    activities:   activities.rows,
    appointments: appointments.rows,
    tasks:        tasks.rows,
    lead_notes:   notes.rows,
  });
}));

app.post('/api/leads', asyncHandler(async (req, res) => {
  const { name, last_name, company, email, phone, channel, status, priority, estimated_value, notes, agent } = req.body;
  if (!name) return res.status(400).json({ error: 'El nombre es requerido' });

  const { rows: [lead] } = await query(
    `INSERT INTO leads (id, name, last_name, company, email, phone, channel, status, priority, estimated_value, notes, agent)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
    [uuidv4(), name, company, email, phone,
     channel  || 'Web',
     status   || 'Nuevo',
     priority || 'Media',
     parseFloat(estimated_value) || 0,
     notes, agent]
  );
  res.status(201).json(lead);
}));

app.put('/api/leads/:id', asyncHandler(async (req, res) => {
  const { name, last_name, company, email, phone, channel, status, priority, estimated_value, notes, agent } = req.body;
  const { rows: [lead] } = await query(
    `UPDATE leads
     SET name=$1, company=$2, email=$3, phone=$4, channel=$5,
         status=$6, priority=$7, estimated_value=$8, notes=$9, agent=$10,
         updated_at=NOW()
     WHERE id=$11 RETURNING *`,
    [name, company, email, phone, channel, status, priority,
     parseFloat(estimated_value) || 0, notes, agent, req.params.id]
  );
  if (!lead) return res.status(404).json({ error: 'Lead no encontrado' });
  res.json(lead);
}));

app.delete('/api/leads/:id', asyncHandler(async (req, res) => {
  await query('DELETE FROM leads WHERE id = $1', [req.params.id]);
  res.json({ success: true });
}));

// ─── PIPELINE ─────────────────────────────────────────────────────
app.get('/api/pipeline', asyncHandler(async (req, res) => {
  const stages = ['Nuevo','Contactado','Calificado','Propuesta','Negociacion','Ganado','Perdido'];
  const { rows } = await query(
    `SELECT * FROM leads WHERE status = ANY($1) ORDER BY estimated_value DESC`,
    [stages]
  );

  const pipeline = {};
  stages.forEach(stage => {
    const leads = rows.filter(l => l.status === stage);
    pipeline[stage] = {
      leads,
      count: leads.length,
      total: leads.reduce((s, l) => s + parseFloat(l.estimated_value || 0), 0),
    };
  });
  res.json(pipeline);
}));

app.patch('/api/leads/:id/status', asyncHandler(async (req, res) => {
  const { status } = req.body;
  await query('UPDATE leads SET status=$1, updated_at=NOW() WHERE id=$2', [status, req.params.id]);
  res.json({ success: true });
}));

// ─── ACTIVIDADES ──────────────────────────────────────────────────
app.post('/api/leads/:id/activities', asyncHandler(async (req, res) => {
  const { type, content, channel, direction } = req.body;
  const { rows: [act] } = await query(
    `INSERT INTO activities (id, lead_id, type, content, channel, direction)
     VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
    [uuidv4(), req.params.id, type, content, channel, direction || 'outbound']
  );
  res.status(201).json(act);
}));

// ─── CITAS ────────────────────────────────────────────────────────
app.post('/api/leads/:id/appointments', asyncHandler(async (req, res) => {
  const { scheduled_at, channel, notes } = req.body;
  const { rows: [appt] } = await query(
    `INSERT INTO appointments (id, lead_id, scheduled_at, channel, notes)
     VALUES ($1,$2,$3,$4,$5) RETURNING *`,
    [uuidv4(), req.params.id, scheduled_at, channel, notes]
  );
  res.status(201).json(appt);
}));

// ─── TAREAS ───────────────────────────────────────────────────────
app.post('/api/leads/:id/tasks', asyncHandler(async (req, res) => {
  const { title, due_date } = req.body;
  const { rows: [task] } = await query(
    `INSERT INTO tasks (id, lead_id, title, due_date) VALUES ($1,$2,$3,$4) RETURNING *`,
    [uuidv4(), req.params.id, title, due_date || null]
  );
  res.status(201).json(task);
}));

app.patch('/api/tasks/:id/complete', asyncHandler(async (req, res) => {
  await query('UPDATE tasks SET completed = TRUE WHERE id = $1', [req.params.id]);
  res.json({ success: true });
}));

// ─── NOTAS ────────────────────────────────────────────────────────
app.post('/api/leads/:id/notes', asyncHandler(async (req, res) => {
  const { content } = req.body;
  const { rows: [note] } = await query(
    `INSERT INTO notes (id, lead_id, content) VALUES ($1,$2,$3) RETURNING *`,
    [uuidv4(), req.params.id, content]
  );
  res.status(201).json(note);
}));

// ─── WEBHOOK AGENTES ──────────────────────────────────────────────
// OLD WEBHOOK REMOVED

// ─── START ────────────────────────────────────────────────────────
initDB().then(() => {
  app.listen(PORT, () => console.log(`LeadFlow API en puerto ${PORT} [${process.env.NODE_ENV || 'development'}]`));
}).catch(err => {
  console.error('Error al iniciar:', err);
  process.exit(1);
});

// ── AUTH ─────────────────────────────────────────────────────
const jwt = require('jsonwebtoken');
const JWT_SECRET = process.env.JWT_SECRET || 'leadflow_secret_2024';

app.post('/api/auth/login', asyncHandler(async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'Email y contraseña requeridos' });

  const { rows: [user] } = await query(
    `SELECT * FROM users WHERE email = $1 AND active = TRUE`, [email]
  );

  if (!user) return res.status(401).json({ error: 'Credenciales incorrectas' });

  const { rows: [valid] } = await query(
    `SELECT (password_hash = crypt($1, password_hash)) as ok FROM users WHERE id = $2`,
    [password, user.id]
  );

  if (!valid.ok) return res.status(401).json({ error: 'Credenciales incorrectas' });

  const token = jwt.sign({ id: user.id, email: user.email, role: user.role }, JWT_SECRET, { expiresIn: '7d' });

  res.json({ token, user: { id: user.id, name: user.name, email: user.email, role: user.role } });
}));

// ── QUOTES ───────────────────────────────────────────────────
app.get('/api/quotes', asyncHandler(async (req, res) => {
  const { status } = req.query;
  const conds = ['1=1']; const params = []; let i = 1;
  if (status) { conds.push(`q.status = $${i++}`); params.push(status); }
  const { rows } = await query(
    `SELECT q.*, l.name as lead_name, l.company
     FROM quotes q JOIN leads l ON l.id = q.lead_id
     WHERE ${conds.join(' AND ')} ORDER BY q.created_at DESC LIMIT 100`,
    params
  );
  res.json(rows);
}));

app.post('/api/leads/:id/quotes', asyncHandler(async (req, res) => {
  const { iva_rate = 19, valid_until, notes, items = [] } = req.body;
  const { rows: [quote] } = await query(
    `INSERT INTO quotes (lead_id, iva_rate, valid_until, notes)
     VALUES ($1,$2,$3,$4) RETURNING *`,
    [req.params.id, iva_rate, valid_until || null, notes]
  );
  for (let i = 0; i < items.length; i++) {
    const it = items[i];
    await query(
      `INSERT INTO quote_items (quote_id, product, description, quantity, unit_price, ord)
       VALUES ($1,$2,$3,$4,$5,$6)`,
      [quote.id, it.product, it.description || null, it.quantity || 1, it.unit_price || 0, i]
    );
  }
  const { rows: [updated] } = await query('SELECT * FROM quotes WHERE id = $1', [quote.id]);
  await query('UPDATE leads SET estimated_value = (SELECT COALESCE(SUM(total),0) FROM quotes WHERE lead_id = $1 AND status != $2) WHERE id = $1', [req.params.id, 'rechazada']);
  res.status(201).json(updated);
}));

app.patch('/api/quotes/:id/status', asyncHandler(async (req, res) => {
  const { status } = req.body;
  const { rows: [q] } = await query(
    'UPDATE quotes SET status=$1, updated_at=NOW() WHERE id=$2 RETURNING *',
    [status, req.params.id]
  );
  res.json(q);
}));

app.get('/api/quotes/:id', asyncHandler(async (req, res) => {
  const { rows: [quote] } = await query('SELECT * FROM quotes WHERE id = $1', [req.params.id]);
  if (!quote) return res.status(404).json({ error: 'Cotizacion no encontrada' });
  const [items, lead] = await Promise.all([
    query('SELECT * FROM quote_items WHERE quote_id = $1 ORDER BY ord ASC', [req.params.id]),
    query('SELECT id, name, company, email, phone FROM leads WHERE id = $1', [quote.lead_id]),
  ]);
  res.json({ ...quote, items: items.rows, lead: lead.rows[0] });
}));

app.post('/api/leads/v2', asyncHandler(async (req, res) => {
  const { name, last_name, company, email, phone, channel, status, priority, estimated_value, notes, agent } = req.body;
  if (!name) return res.status(400).json({ error: 'Nombre requerido' });
  const full_name = last_name ? `${name} ${last_name}` : name;
  const { rows: [lead] } = await query(
    `INSERT INTO leads (id,name,last_name,company,email,phone,channel,status,priority,estimated_value,notes,agent)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
    [require('crypto').randomUUID(), full_name, last_name||null, company, email, phone,
     channel||'Web', status||'Nuevo', priority||'Media',
     parseFloat(estimated_value)||0, notes, agent]
  );
  res.status(201).json(lead);
}));

app.get('/api/leads/:id/quotes', asyncHandler(async (req, res) => {
  const { rows } = await query(
    `SELECT * FROM quotes WHERE lead_id = $1 ORDER BY created_at DESC`,
    [req.params.id]
  );
  res.json(rows);
}));

app.get('/api/leads/:id/notes', asyncHandler(async (req, res) => {
  const { rows } = await query(
    'SELECT * FROM notes WHERE lead_id = $1 ORDER BY created_at DESC',
    [req.params.id]
  );
  res.json(rows);
}));

app.get('/api/leads/:id/notes-list', asyncHandler(async (req, res) => {
  const { rows } = await query(
    'SELECT * FROM notes WHERE lead_id = $1 ORDER BY created_at DESC',
    [req.params.id]
  );
  res.json(rows);
}));

// ── SETTINGS ─────────────────────────────────────────────────
app.get('/api/settings', asyncHandler(async (req, res) => {
  const { rows } = await query('SELECT key, value FROM settings ORDER BY key');
  const obj = {};
  rows.forEach(r => { obj[r.key] = r.value; });
  res.json(obj);
}));

app.put('/api/settings', asyncHandler(async (req, res) => {
  const entries = Object.entries(req.body);
  for (const [key, value] of entries) {
    await query(
      `INSERT INTO settings (key, value, updated_at) VALUES ($1,$2,NOW())
       ON CONFLICT (key) DO UPDATE SET value=$2, updated_at=NOW()`,
      [key, value || '']
    );
  }
  res.json({ success: true });
}));

// ── UPLOAD IMAGEN ─────────────────────────────────────────────
const multer = require('multer');
const path   = require('path');
const fs     = require('fs');

const uploadsDir = process.env.UPLOADS_DIR || path.join(__dirname, 'uploads');
const uploadsProductsDir = path.join(uploadsDir, 'products');
if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });
if (!fs.existsSync(uploadsProductsDir)) fs.mkdirSync(uploadsProductsDir, { recursive: true });

const BASE_URL = process.env.BASE_URL || `http://localhost:${PORT}`;

app.use('/uploads', express.static(uploadsDir));

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadsDir),
  filename:    (req, file, cb) => {
    const ext = path.extname(file.originalname);
    cb(null, 'logo-' + Date.now() + ext);
  }
});

const upload = multer({
  storage,
  limits: { fileSize: 2 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowed = ['.jpg','.jpeg','.png','.svg','.webp'];
    const ext = path.extname(file.originalname).toLowerCase();
    if (allowed.includes(ext)) cb(null, true);
    else cb(new Error('Solo se permiten imágenes'));
  }
});

app.post('/api/upload/logo', upload.single('logo'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se subió ningún archivo' });
  const url = `${BASE_URL}/uploads/${req.file.filename}`;
  res.json({ url });
});

// ── WEBHOOK AGENTE ────────────────────────────────────────────
app.post('/api/webhook/agent', asyncHandler(async (req, res) => {
  const { type, phone, contact_phone, name, contact_name, message, agent, order } = req.body;
  const phoneNum = phone || contact_phone;
  const nameStr = name || contact_name || phoneNum;

  // Buscar o crear lead por teléfono
  let lead;
  const { rows: existing } = await query(
    'SELECT * FROM leads WHERE phone = $1 LIMIT 1', [phoneNum]
  );

  if (existing.length > 0) {
    lead = existing[0];
    // Actualizar datos si vienen nuevos
    const updates = ['updated_at=NOW()'];
    const vals = [lead.id];
    let idx = 2;
    const { email: emailNew, rut, last_name: lastName } = req.body;
    if (emailNew && !lead.email) { updates.push(`email=$${idx++}`); vals.push(emailNew); }
    if (lastName && !lead.last_name) { updates.push(`last_name=$${idx++}`); vals.push(lastName); }
    if (rut && !lead.rut) { updates.push(`rut=$${idx++}`); vals.push(rut); }
    await query(`UPDATE leads SET ${updates.join(',')} WHERE id=$1`, vals);
  } else {
    // Crear nuevo lead con todos los datos
    const { email: emailNew, rut, last_name: lastName } = req.body;
    const { rows: [newLead] } = await query(
      `INSERT INTO leads (id, name, last_name, phone, email, rut, channel, status, priority, agent, notes)
       VALUES ($1,$2,$3,$4,$5,$6,'WhatsApp','Nuevo','Media',$7,$8) RETURNING *`,
      [require('crypto').randomUUID(), nameStr, lastName||null, phoneNum,
       emailNew||null, rut||null, agent||'Nico', message||'']
    );
    lead = newLead;
  }

  // Registrar actividad
  if (message) {
    const { direction: msgDirection = 'inbound' } = req.body;
    await query(
      `INSERT INTO activities (id, lead_id, type, content, channel, direction)
       VALUES ($1,$2,'Mensaje',$3,'WhatsApp',$4)`,
      [require('crypto').randomUUID(), lead.id, message, msgDirection]
    );
  }

  // Si hay orden confirmada, guardar en tabla orders y actualizar lead
  if (order && order.total) {
    await query(
      'UPDATE leads SET estimated_value = $1, status = $2 WHERE id = $3',
      [order.total, 'Propuesta', lead.id]
    );
    // Guardar orden detallada
    await query(
      `INSERT INTO orders (id, lead_id, nro_orden, marca, modelo, medida, cantidad, 
       precio_unitario, total, tipo_servicio, fecha_entrega, direccion, comuna)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
      [require('crypto').randomUUID(), lead.id,
       order.nro_orden || ('ORD-' + Date.now()),
       order.marca, order.modelo, order.medida,
       order.cantidad, order.precio_unitario, order.total,
       order.tipo_servicio, order.fecha_entrega,
       order.direccion, order.comuna]
    );
    // Limpiar notas — no poner el resumen de la orden ahí
    await query(
      "UPDATE leads SET notes = NULL WHERE id = $1 AND notes LIKE 'Orden confirmada%'",
      [lead.id]
    );
  }

  res.json({ success: true, lead_id: lead.id });
}));

// ── CONTACTOS ─────────────────────────────────────────────────
app.get('/api/contacts', asyncHandler(async (req, res) => {
  const { search } = req.query;
  let q = `SELECT id, name, last_name, email, phone, rut, company, company_rut, 
           job_title, channel, status, estimated_value, created_at, updated_at
           FROM leads`;
  const params = [];
  if (search) {
    q += ` WHERE name ILIKE $1 OR email ILIKE $1 OR phone ILIKE $1 OR company ILIKE $1 OR rut ILIKE $1`;
    params.push('%' + search + '%');
  }
  q += ' ORDER BY name ASC';
  const { rows } = await query(q, params);
  res.json(rows);
}));

app.post('/api/contacts', asyncHandler(async (req, res) => {
  const { name, last_name, email, phone, rut, company, company_rut, job_title, channel } = req.body;
  if (!name) return res.status(400).json({ error: 'Nombre requerido' });
  const { rows: [contact] } = await query(
    `INSERT INTO leads (id, name, last_name, email, phone, rut, company, company_rut, job_title, channel, status, priority)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'Nuevo','Media') RETURNING *`,
    [require('crypto').randomUUID(), name, last_name||null, email||null, phone||null,
     rut||null, company||null, company_rut||null, job_title||null, channel||'Web']
  );
  res.status(201).json(contact);
}));

app.put('/api/contacts/:id', asyncHandler(async (req, res) => {
  const { name, last_name, email, phone, rut, company, company_rut, job_title, channel } = req.body;
  const { rows: [contact] } = await query(
    `UPDATE leads SET name=$1, last_name=$2, email=$3, phone=$4, rut=$5,
     company=$6, company_rut=$7, job_title=$8, channel=$9, updated_at=NOW()
     WHERE id=$10 RETURNING *`,
    [name, last_name||null, email||null, phone||null, rut||null,
     company||null, company_rut||null, job_title||null, channel||'Web', req.params.id]
  );
  res.json(contact);
}));

app.delete('/api/contacts/:id', asyncHandler(async (req, res) => {
  await query('DELETE FROM leads WHERE id = $1', [req.params.id]);
  res.json({ success: true });
}));

// ── GENERAR PDF COTIZACIÓN ────────────────────────────────────
app.get('/api/quotes/:id/pdf', asyncHandler(async (req, res) => {
  const { rows: [quote] } = await query('SELECT * FROM quotes WHERE id = $1', [req.params.id]);
  if (!quote) return res.status(404).json({ error: 'Cotización no encontrada' });

  const [items, lead, settingsRows] = await Promise.all([
    query('SELECT * FROM quote_items WHERE quote_id = $1 ORDER BY ord ASC', [req.params.id]),
    query('SELECT name, last_name, email, phone, rut, company FROM leads WHERE id = $1', [quote.lead_id]),
    query('SELECT key, value FROM settings'),
  ]);

  const cfg = {};
  settingsRows.rows.forEach(r => { cfg[r.key] = r.value; });
  const client = lead.rows[0] || {};

  const fmt = (n) => new Intl.NumberFormat('es-CL', { style:'currency', currency:'CLP', maximumFractionDigits:0 }).format(n||0);
  const fmtDate = (d) => d ? new Date(d).toLocaleDateString('es-CL', { day:'2-digit', month:'long', year:'numeric' }) : '—';

  const PDFDocument = require('pdfkit');
  const doc = new PDFDocument({ margin: 50, size: 'A4' });

  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${quote.quote_number}.pdf"`);
  doc.pipe(res);

  const blue   = '#2563eb';
  const gray   = '#6b7280';
  const dark   = '#111827';
  const light  = '#f3f4f6';
  const W      = 495; // usable width

  // ── HEADER ──────────────────────────────────────────
  doc.rect(50, 40, W, 80).fill(blue);
  
  // Logo text or company name
  doc.fillColor('#ffffff').font('Helvetica-Bold').fontSize(20)
     .text(cfg.company_name || 'Mi Empresa', 65, 55, { width: 200 });
  
  if (cfg.company_rut) {
    doc.font('Helvetica').fontSize(9).fillColor('#bfdbfe')
       .text('RUT: ' + cfg.company_rut, 65, 80);
  }
  if (cfg.company_phone) {
    doc.font('Helvetica').fontSize(9).fillColor('#bfdbfe')
       .text(cfg.company_phone, 65, 92);
  }

  // Quote number on right
  doc.font('Helvetica-Bold').fontSize(22).fillColor('#ffffff')
     .text('COTIZACIÓN', 300, 50, { width: 230, align: 'right' });
  doc.font('Helvetica-Bold').fontSize(14).fillColor('#bfdbfe')
     .text(quote.quote_number, 300, 76, { width: 230, align: 'right' });
  doc.font('Helvetica').fontSize(9).fillColor('#93c5fd')
     .text('Fecha: ' + fmtDate(quote.created_at), 300, 95, { width: 230, align: 'right' });

  doc.moveDown(4);

  // ── CLIENTE + INFO ──────────────────────────────────
  const infoY = 140;
  
  // Left box - Cliente
  doc.rect(50, infoY, 235, 90).fill(light);
  doc.font('Helvetica-Bold').fontSize(8).fillColor(gray)
     .text('CLIENTE', 62, infoY + 10);
  doc.font('Helvetica-Bold').fontSize(11).fillColor(dark)
     .text((client.name||'') + ' ' + (client.last_name||''), 62, infoY + 22, { width: 210 });
  let cy = infoY + 36;
  if (client.rut)     { doc.font('Helvetica').fontSize(9).fillColor(gray).text('RUT: ' + client.rut, 62, cy); cy += 12; }
  if (client.company) { doc.font('Helvetica').fontSize(9).fillColor(gray).text(client.company, 62, cy); cy += 12; }
  if (client.email)   { doc.font('Helvetica').fontSize(9).fillColor(gray).text(client.email, 62, cy); cy += 12; }
  if (client.phone)   { doc.font('Helvetica').fontSize(9).fillColor(gray).text(client.phone, 62, cy); }

  // Right box - Detalle
  doc.rect(295, infoY, 250, 90).fill(light);
  doc.font('Helvetica-Bold').fontSize(8).fillColor(gray)
     .text('DETALLE', 307, infoY + 10);
  doc.font('Helvetica').fontSize(9).fillColor(dark)
     .text('N° cotización: ', 307, infoY + 22)
     .font('Helvetica-Bold').text(quote.quote_number, 390, infoY + 22);
  doc.font('Helvetica').fontSize(9).fillColor(dark)
     .text('IVA aplicado: ', 307, infoY + 36)
     .font('Helvetica-Bold').text(quote.iva_rate + '%', 390, infoY + 36);
  if (quote.valid_until) {
    doc.font('Helvetica').fontSize(9).fillColor(dark)
       .text('Vence: ', 307, infoY + 50)
       .font('Helvetica-Bold').fillColor('#dc2626').text(fmtDate(quote.valid_until), 390, infoY + 50);
  }
  doc.font('Helvetica').fontSize(9).fillColor(dark)
     .text('Estado: ', 307, infoY + 64)
     .font('Helvetica-Bold').fillColor(blue).text(quote.status.toUpperCase(), 390, infoY + 64);

  // ── TABLA PRODUCTOS ─────────────────────────────────
  // Buscar fotos de productos en el catálogo
  const productPhotos = {};
  for (const it of items.rows) {
    try {
      const { rows: prods } = await query(
        `SELECT photo_url FROM products WHERE name ILIKE $1 AND photo_url IS NOT NULL AND photo_url != '' LIMIT 1`,
        [it.product]
      );
      if (prods.length > 0) productPhotos[it.id] = prods[0].photo_url;
    } catch(e) {}
  }

  // Descargar imágenes
  const fetch = require('node-fetch');
  const imageBuffers = {};
  for (const [itemId, url] of Object.entries(productPhotos)) {
    try {
      const r = await fetch(url, { timeout: 3000 });
      if (r.ok) imageBuffers[itemId] = await r.buffer();
    } catch(e) {}
  }

  const hasPhotos = Object.keys(imageBuffers).length > 0;
  const rowH = hasPhotos ? 40 : 22;
  const tableY = infoY + 105;
  const cols = hasPhotos
    ? { num:50, foto:65, prod:115, desc:260, cant:330, precio:395, sub:460 }
    : { num:50, prod:105, desc:240, cant:320, precio:390, sub:460 };

  // Header
  doc.rect(50, tableY, W, 22).fill(blue);
  doc.font('Helvetica-Bold').fontSize(8).fillColor('#ffffff');
  doc.text('#', cols.num, tableY + 7);
  if (hasPhotos) doc.text('', cols.foto, tableY + 7);
  doc.text('Producto',    cols.prod,  tableY + 7);
  doc.text('Descripción', cols.desc,  tableY + 7);
  doc.text('Cant.',       cols.cant,  tableY + 7);
  doc.text('P. Unit.',    cols.precio,tableY + 7);
  doc.text('Subtotal',    cols.sub,   tableY + 7);

  // Rows
  let rowY = tableY + 22;
  for (let i = 0; i < items.rows.length; i++) {
    const it = items.rows[i];
    const bg = i % 2 === 0 ? '#ffffff' : '#f9fafb';
    doc.rect(50, rowY, W, rowH).fill(bg);

    const textY = rowY + (rowH === 40 ? 8 : 7);

    // Foto
    if (hasPhotos && imageBuffers[it.id]) {
      try {
        doc.image(imageBuffers[it.id], cols.foto, rowY + 3, { width: 34, height: 34, fit: [34,34] });
      } catch(e) {}
    }

    doc.font('Helvetica').fontSize(8).fillColor(dark)
       .text(String(i+1), cols.num, textY);
    doc.font('Helvetica-Bold').fillColor(dark)
       .text(it.product, cols.prod, textY, { width: hasPhotos ? 140 : 130 });
    doc.font('Helvetica').fillColor(gray)
       .text(it.description || '—', cols.desc, textY, { width: 65 });
    doc.fillColor(dark)
       .text(String(it.quantity), cols.cant, textY);
    doc.text(fmt(it.unit_price), cols.precio, textY);
    doc.font('Helvetica-Bold')
       .text(fmt(it.subtotal), cols.sub, textY);
    rowY += rowH;
  }

  // ── TOTALES ─────────────────────────────────────────
  rowY += 10;
  const totX = 350;

  doc.font('Helvetica').fontSize(9).fillColor(gray)
     .text('Total Neto:', totX, rowY)
     .font('Helvetica').fillColor(dark)
     .text(fmt(quote.subtotal), totX + 100, rowY, { width: 90, align: 'right' });

  rowY += 16;
  doc.font('Helvetica').fontSize(9).fillColor(gray)
     .text(`IVA (${quote.iva_rate}%):`, totX, rowY)
     .font('Helvetica').fillColor(gray)
     .text(fmt(quote.iva_amount), totX + 100, rowY, { width: 90, align: 'right' });

  rowY += 14;
  doc.rect(totX - 5, rowY, 200, 26).fill(blue);
  doc.font('Helvetica-Bold').fontSize(11).fillColor('#ffffff')
     .text('Total con IVA:', totX, rowY + 7)
     .text(fmt(quote.total), totX + 100, rowY + 7, { width: 90, align: 'right' });
  rowY += 40;

  // ── NOTAS ───────────────────────────────────────────
  if (quote.notes) {
    doc.rect(50, rowY, W, 40).fill('#fffbeb');
    doc.font('Helvetica-Bold').fontSize(8).fillColor('#92400e')
       .text('NOTAS Y CONDICIONES', 62, rowY + 8);
    doc.font('Helvetica').fontSize(8).fillColor('#78350f')
       .text(quote.notes, 62, rowY + 20, { width: W - 24 });
    rowY += 50;
  }

  // ── DATOS BANCARIOS ─────────────────────────────────
  if (cfg.bank_name || cfg.bank_account_number) {
    doc.rect(50, rowY, W, 10).fill(light);
    doc.font('Helvetica-Bold').fontSize(8).fillColor(gray)
       .text('DATOS PARA TRANSFERENCIA', 62, rowY + 2);
    rowY += 14;
    const bankFields = [
      ['Banco', cfg.bank_name],
      ['Tipo de cuenta', cfg.bank_account_type],
      ['N° cuenta', cfg.bank_account_number],
      ['Titular', cfg.bank_account_name],
      ['RUT', cfg.bank_rut],
      ['Email confirmación', cfg.bank_email],
    ].filter(([,v]) => v);
    
    bankFields.forEach(([label, value]) => {
      doc.font('Helvetica').fontSize(9).fillColor(gray).text(label + ':', 62, rowY)
         .font('Helvetica-Bold').fillColor(dark).text(value, 200, rowY);
      rowY += 14;
    });
    rowY += 6;
  }

  // ── LINKS DE PAGO ────────────────────────────────────
  const links = [1,2,3].map(n => ({
    label: cfg[`payment_link_${n}_label`],
    url:   cfg[`payment_link_${n}_url`],
  })).filter(l => l.label && l.url);

  if (links.length > 0) {
    doc.font('Helvetica-Bold').fontSize(9).fillColor(dark)
       .text('Pagar en línea:', 62, rowY);
    rowY += 14;
    links.forEach(l => {
      doc.font('Helvetica').fontSize(9).fillColor(blue)
         .text('• ' + l.label + ': ' + l.url, 62, rowY, { link: l.url, underline: true });
      rowY += 14;
    });
  }

  // ── FOOTER ───────────────────────────────────────────
  doc.rect(50, 780, W, 1).fill('#e5e7eb');
  doc.font('Helvetica').fontSize(8).fillColor(gray)
     .text(cfg.quote_footer || 'Gracias por su preferencia.', 50, 788, { width: W, align: 'center' });
  if (cfg.company_website) {
    doc.fillColor(blue).text(cfg.company_website, 50, 800, { width: W, align: 'center', link: cfg.company_website });
  }

  doc.end();
}));
// ── ORDERS ────────────────────────────────────────────────────
app.get('/api/leads/:id/orders', asyncHandler(async (req, res) => {
  const { rows } = await query(
    'SELECT * FROM orders WHERE lead_id = $1 ORDER BY created_at DESC',
    [req.params.id]
  );
  res.json(rows);
}));

app.patch('/api/orders/:id/status', asyncHandler(async (req, res) => {
  const { status } = req.body;
  const { rows: [order] } = await query(
    'UPDATE orders SET status=$1 WHERE id=$2 RETURNING *',
    [status, req.params.id]
  );
  res.json(order);
}));

app.get('/api/orders', asyncHandler(async (req, res) => {
  const { status } = req.query;
  let q = `SELECT o.*, 
    l.name as lead_name, l.last_name, l.phone, l.email, l.company
    FROM orders o
    LEFT JOIN leads l ON l.id = o.lead_id`;
  const params = [];
  if (status) { q += ' WHERE o.status = $1'; params.push(status); }
  q += ' ORDER BY o.created_at DESC';
  const { rows } = await query(q, params);
  res.json(rows);
}));

// ── CATÁLOGO ──────────────────────────────────────────────────
app.get('/api/catalog', asyncHandler(async (req, res) => {
  const { search, category, active = 'true', page = 1, limit = 50 } = req.query;
  const offset = (parseInt(page) - 1) * parseInt(limit);

  let where = 'WHERE p.active = $1';
  const params = [active === 'true'];
  let i = 2;

  if (search) {
    where += ` AND (p.name ILIKE $${i} OR p.brand ILIKE $${i} OR p.description ILIKE $${i})`;
    params.push('%' + search + '%');
    i++;
  }
  if (category) {
    where += ` AND p.category = $${i}`;
    params.push(category);
    i++;
  }
  if (req.query.brand) {
    where += ` AND p.brand = $${i}`;
    params.push(req.query.brand);
    i++;
  }
  if (req.query.stock) {
    where += ` AND p.stock > 0`;
  }
  if (req.query.no_photo) {
    where += ` AND (p.photo_url IS NULL OR p.photo_url = '')`;
  }
  if (req.query.ancho) {
    where += ` AND EXISTS (SELECT 1 FROM product_values WHERE product_id=p.id AND field_key='ancho' AND field_value=$${i})`;
    params.push(req.query.ancho);
    i++;
  }
  if (req.query.perfil) {
    where += ` AND EXISTS (SELECT 1 FROM product_values WHERE product_id=p.id AND field_key='perfil' AND field_value=$${i})`;
    params.push(req.query.perfil);
    i++;
  }
  if (req.query.aro) {
    where += ` AND EXISTS (SELECT 1 FROM product_values WHERE product_id=p.id AND field_key='aro' AND field_value=$${i})`;
    params.push(req.query.aro);
    i++;
  }

  const [dataResult, countResult] = await Promise.all([
    query(
      `SELECT p.*, 
        json_object_agg(pv.field_key, pv.field_value) FILTER (WHERE pv.field_key IS NOT NULL) as custom_fields
       FROM products p
       LEFT JOIN product_values pv ON pv.product_id = p.id
       ${where}
       GROUP BY p.id 
       ORDER BY p.name ASC
       LIMIT $${i} OFFSET $${i+1}`,
      [...params, parseInt(limit), offset]
    ),
    query(
      `SELECT COUNT(DISTINCT p.id) as total FROM products p LEFT JOIN product_values pv ON pv.product_id = p.id ${where}`,
      params
    )
  ]);

  const total = parseInt(countResult.rows[0]?.total || 0);
  res.json({
    products: dataResult.rows,
    total,
    page: parseInt(page),
    limit: parseInt(limit),
    pages: Math.ceil(total / parseInt(limit))
  });
}));
app.get('/api/catalog/fields', asyncHandler(async (req, res) => {
  const { rows } = await query('SELECT * FROM catalog_fields ORDER BY ord ASC');
  res.json(rows);
}));

app.post('/api/catalog/fields', asyncHandler(async (req, res) => {
  const { field_key, label, field_type, options, required, ord } = req.body;
  const { rows: [field] } = await query(
    `INSERT INTO catalog_fields (id, field_key, label, field_type, options, required, ord)
     VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
    [require('crypto').randomUUID(), field_key, label, field_type||'text', options||null, required||false, ord||0]
  );
  res.status(201).json(field);
}));

app.delete('/api/catalog/fields/:key', asyncHandler(async (req, res) => {
  await query('DELETE FROM catalog_fields WHERE field_key = $1', [req.params.key]);
  res.json({ success: true });
}));

// Descargar template Excel
app.get('/api/catalog/template', asyncHandler(async (req, res) => {
  const XLSX = require('xlsx');
  const { rows: fields } = await query('SELECT field_key, label FROM catalog_fields ORDER BY ord');

  const headers = [
    'name', 'description', 'brand', 'category', 'unit',
    'price_normal', 'price_offer', 'stock', 'photo_url',
    ...fields.map(f => f.field_key)
  ];

  const example = {
    name: 'Bridgestone Turanza T005',
    description: 'Neumático de alto rendimiento',
    brand: 'Bridgestone',
    category: 'Neumático',
    unit: 'unidad',
    price_normal: 89990,
    price_offer: 79990,
    stock: 10,
    photo_url: 'https://...',
  };
  fields.forEach(f => { example[f.field_key] = ''; });

  const ws = XLSX.utils.json_to_sheet([example], { header: headers });
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Productos');

  // Ajustar anchos
  ws['!cols'] = headers.map(() => ({ wch: 20 }));

  const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', 'attachment; filename="template_catalogo.xlsx"');
  res.send(buffer);
}));


// ── IMPORTAR CATÁLOGO DESDE EXCEL ────────────────────────────
const multer2 = require('multer');
const uploadExcel = multer2({ storage: multer2.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

app.post('/api/catalog/import', uploadExcel.single('file'), asyncHandler(async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se subió ningún archivo' });

  const XLSX = require('xlsx');
  const workbook = XLSX.read(req.file.buffer, { type: 'buffer' });
  const sheet    = workbook.Sheets[workbook.SheetNames[0]];
  const rows     = XLSX.utils.sheet_to_json(sheet, { defval: '' });

  if (rows.length === 0) return res.status(400).json({ error: 'El archivo está vacío' });

  // Obtener campos personalizados definidos
  const { rows: fieldDefs } = await query('SELECT field_key FROM catalog_fields');
  const validKeys = fieldDefs.map(f => f.field_key);

  const BASE_FIELDS = ['name','description','category','brand','unit','price_normal','price_offer','stock','photo_url'];

  let created = 0, updated = 0, errors = [];

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    try {
      const name = String(row.name || row.nombre || row.Nombre || '').trim();
      if (!name) { errors.push(`Fila ${i+2}: sin nombre`); continue; }

      const base = {
        name,
        description:  String(row.description  || row.descripcion  || ''),
        category:     String(row.category      || row.categoria    || ''),
        brand:        String(row.brand         || row.marca        || ''),
        unit:         String(row.unit          || row.unidad       || 'unidad'),
        price_normal: parseFloat(row.price_normal || row.precio_normal || row.precio || 0) || 0,
        price_offer:  parseFloat(row.price_offer  || row.precio_oferta || 0) || null,
        stock:        parseInt(row.stock        || 0) || 0,
        photo_url:    String(row.photo_url || row.foto || '').split('|')[0].trim(),
      };

      // Campos personalizados
      const custom = {};
      for (const key of validKeys) {
        if (row[key] !== undefined && row[key] !== '') {
          custom[key] = String(row[key]);
        }
      }

      // Verificar si existe por nombre + marca
      const { rows: existing } = await query(
        'SELECT id FROM products WHERE name = $1 AND (brand = $2 OR $2 = \'\')',
        [base.name, base.brand]
      );

      let productId;
      if (existing.length > 0) {
        productId = existing[0].id;
        await query(
          `UPDATE products SET description=$1, category=$2, brand=$3, unit=$4,
           price_normal=$5, price_offer=$6, stock=$7, photo_url=$8, updated_at=NOW()
           WHERE id=$9`,
          [base.description, base.category, base.brand, base.unit,
           base.price_normal, base.price_offer, base.stock, base.photo_url, productId]
        );
        updated++;
      } else {
        const { rows: [p] } = await query(
          `INSERT INTO products (id,name,description,category,brand,unit,price_normal,price_offer,stock,photo_url,active)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,true) RETURNING id`,
          [require('crypto').randomUUID(), base.name, base.description, base.category,
           base.brand, base.unit, base.price_normal, base.price_offer, base.stock, base.photo_url]
        );
        productId = p.id;
        created++;
      }

      // Guardar campos personalizados
      for (const [key, value] of Object.entries(custom)) {
        await query(
          `INSERT INTO product_values (id, product_id, field_key, field_value)
           VALUES ($1,$2,$3,$4) ON CONFLICT (product_id, field_key) DO UPDATE SET field_value=$4`,
          [require('crypto').randomUUID(), productId, key, value]
        );
      }
    } catch (err) {
      errors.push(`Fila ${i+2}: ${err.message}`);
    }
  }

  res.json({ success: true, created, updated, errors, total: rows.length });
}));


app.get('/api/catalog/filter-options', asyncHandler(async (req, res) => {
  const [brands, anchos, perfiles, aros] = await Promise.all([
    query(`SELECT DISTINCT brand FROM products WHERE active=true AND brand IS NOT NULL AND brand != '' ORDER BY brand ASC`),
    query(`SELECT DISTINCT field_value as val FROM product_values WHERE field_key='ancho' AND field_value != '' AND field_value ~ '^[0-9]+$'`),
    query(`SELECT DISTINCT field_value as val FROM product_values WHERE field_key='perfil' AND field_value != '' AND field_value ~ '^[0-9]+$'`),
    query(`SELECT DISTINCT field_value as val FROM product_values WHERE field_key='aro' AND field_value != ''`),
  ]);
  res.json({
    brands:   brands.rows.map(r => r.brand),
    anchos:   anchos.rows.map(r => r.val),
    perfiles: perfiles.rows.map(r => r.val),
    aros:     aros.rows.map(r => r.val),
  });
}));


app.get('/api/catalog/:id', asyncHandler(async (req, res) => {
  const { rows: [product] } = await query('SELECT * FROM products WHERE id = $1', [req.params.id]);
  if (!product) return res.status(404).json({ error: 'Producto no encontrado' });
  const { rows: values } = await query('SELECT * FROM product_values WHERE product_id = $1', [req.params.id]);
  const custom_fields = {};
  values.forEach(v => { custom_fields[v.field_key] = v.field_value; });
  res.json({ ...product, custom_fields });
}));

app.post('/api/catalog', asyncHandler(async (req, res) => {
  const { name, description, category, brand, unit, price_normal, price_offer, stock, photo_url, active, custom_fields } = req.body;
  if (!name) return res.status(400).json({ error: 'Nombre requerido' });
  const { rows: [product] } = await query(
    `INSERT INTO products (id,name,description,category,brand,unit,price_normal,price_offer,stock,photo_url,active)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
    [require('crypto').randomUUID(), name, description||null, category||null, brand||null,
     unit||'unidad', parseFloat(price_normal)||0, price_offer ? parseFloat(price_offer) : null,
     parseInt(stock)||0, photo_url||null, active !== false]
  );
  if (custom_fields) {
    for (const [key, value] of Object.entries(custom_fields)) {
      if (value !== undefined && value !== '') {
        await query(
          `INSERT INTO product_values (id, product_id, field_key, field_value)
           VALUES ($1,$2,$3,$4) ON CONFLICT (product_id, field_key) DO UPDATE SET field_value=$4`,
          [require('crypto').randomUUID(), product.id, key, String(value)]
        );
      }
    }
  }
  res.status(201).json(product);
}));

app.put('/api/catalog/:id', asyncHandler(async (req, res) => {
  const { name, description, category, brand, unit, price_normal, price_offer, stock, photo_url, active, custom_fields } = req.body;
  const { rows: [product] } = await query(
    `UPDATE products SET name=$1, description=$2, category=$3, brand=$4, unit=$5,
     price_normal=$6, price_offer=$7, stock=$8, photo_url=$9, active=$10, updated_at=NOW()
     WHERE id=$11 RETURNING *`,
    [name, description||null, category||null, brand||null, unit||'unidad',
     parseFloat(price_normal)||0, price_offer ? parseFloat(price_offer) : null,
     parseInt(stock)||0, photo_url||null, active !== false, req.params.id]
  );
  if (custom_fields) {
    for (const [key, value] of Object.entries(custom_fields)) {
      await query(
        `INSERT INTO product_values (id, product_id, field_key, field_value)
         VALUES ($1,$2,$3,$4) ON CONFLICT (product_id, field_key) DO UPDATE SET field_value=$4`,
        [require('crypto').randomUUID(), req.params.id, key, String(value||'')]
      );
    }
  }
  res.json(product);
}));

app.delete('/api/catalog/:id', asyncHandler(async (req, res) => {
  await query('DELETE FROM products WHERE id = $1', [req.params.id]);
  res.json({ success: true });
}));




// ── RENTABILIDAD ──────────────────────────────────────────────
app.get('/api/profitability', asyncHandler(async (req, res) => {
  const { search, brand, page = 1, limit = 50 } = req.query;
  const offset = (parseInt(page)-1) * parseInt(limit);
  let where = 'WHERE p.active = true';
  const params = [];
  let i = 1;
  if (search) { where += ` AND (p.name ILIKE $${i} OR p.brand ILIKE $${i})`; params.push('%'+search+'%'); i++; }
  if (brand)  { where += ` AND p.brand = $${i}`; params.push(brand); i++; }
  if (req.query.ancho)  { where += ` AND EXISTS (SELECT 1 FROM product_values WHERE product_id=p.id AND field_key='ancho' AND field_value=$${i})`; params.push(req.query.ancho); i++; }
  if (req.query.perfil) { where += ` AND EXISTS (SELECT 1 FROM product_values WHERE product_id=p.id AND field_key='perfil' AND field_value=$${i})`; params.push(req.query.perfil); i++; }
  if (req.query.aro)    { where += ` AND EXISTS (SELECT 1 FROM product_values WHERE product_id=p.id AND field_key='aro' AND field_value=$${i})`; params.push(req.query.aro); i++; }

  const [data, count] = await Promise.all([
    query(`SELECT p.id, p.name, p.brand, p.category, p.price_normal, p.price_offer, p.cost_price, p.stock,
            json_object_agg(pv.field_key, pv.field_value) FILTER (WHERE pv.field_key IS NOT NULL) as custom_fields
           FROM products p LEFT JOIN product_values pv ON pv.product_id = p.id
           ${where} GROUP BY p.id ORDER BY p.brand ASC, p.name ASC
           LIMIT $${i} OFFSET $${i+1}`, [...params, parseInt(limit), offset]),
    query(`SELECT COUNT(DISTINCT p.id) as total FROM products p ${where}`, params)
  ]);

  res.json({
    products: data.rows,
    total: parseInt(count.rows[0]?.total || 0),
    page: parseInt(page),
    pages: Math.ceil(parseInt(count.rows[0]?.total || 0) / parseInt(limit))
  });
}));

app.patch('/api/profitability/:id/cost', asyncHandler(async (req, res) => {
  const { cost_price } = req.body;
  const { rows: [p] } = await query(
    'UPDATE products SET cost_price=$1 WHERE id=$2 RETURNING id,name,cost_price',
    [parseFloat(cost_price)||0, req.params.id]
  );
  res.json(p);
}));

app.post('/api/profitability/costs/bulk', asyncHandler(async (req, res) => {
  const { costs } = req.body; // [{id, cost_price}]
  for (const { id, cost_price } of costs) {
    await query('UPDATE products SET cost_price=$1 WHERE id=$2', [parseFloat(cost_price)||0, id]);
  }
  res.json({ success: true, updated: costs.length });
}));

// ── TEMPLATE Y CARGA MASIVA DE COSTOS ────────────────────────
app.get('/api/profitability/costs/template', asyncHandler(async (req, res) => {
  const { code_field = 'codigo_interno' } = req.query;
  const XLSX = require('xlsx');

  // Get products with the selected code field
  const { rows } = await query(
    `SELECT p.name, p.brand, pv.field_value as code, p.cost_price
     FROM products p
     LEFT JOIN product_values pv ON pv.product_id = p.id AND pv.field_key = $1
     WHERE p.active = true
     ORDER BY p.brand, p.name`,
    [code_field]
  );

  const label = code_field === 'codigo_interno' ? 'codigo_interno' : 'codigo_proveedor';

  const data = rows.map(r => ({
    [label]:      r.code || '',
    'costo':      r.cost_price || 0,
    'nombre':     r.name,
    'marca':      r.brand,
  }));

  const ws = XLSX.utils.json_to_sheet(data, { header: [label, 'costo', 'nombre', 'marca'] });
  ws['!cols'] = [{ wch: 25 }, { wch: 12 }, { wch: 50 }, { wch: 20 }];

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Costos');

  const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="costos_${label}.xlsx"`);
  res.send(buffer);
}));

app.post('/api/profitability/costs/import', uploadExcel.single('file'), asyncHandler(async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se subió ningún archivo' });
  const { code_field = 'codigo_interno' } = req.body;

  const XLSX = require('xlsx');
  const workbook = XLSX.read(req.file.buffer, { type: 'buffer' });
  const rows = XLSX.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]], { defval: '' });

  if (!rows.length) return res.status(400).json({ error: 'Archivo vacío' });

  let updated = 0, notFound = 0, errors = [];

  for (const row of rows) {
    const code = String(row.codigo_interno || row.codigo_proveedor || '').trim();
    const cost  = parseFloat(row.costo || 0);
    if (!code && !sku) continue;
    const searchCode = code || sku;
    const searchField = sku && !code ? 'codigo_sku' : 'codigo_proveedor';

    try {
      const { rows: products } = await query(
        `SELECT p.id FROM products p
         JOIN product_values pv ON pv.product_id = p.id
         WHERE pv.field_key = $1 AND pv.field_value = $2 LIMIT 1`,
        [code_field, code]
      );

      if (products.length > 0) {
        await query('UPDATE products SET cost_price=$1 WHERE id=$2', [cost, products[0].id]);
        updated++;
      } else {
        notFound++;
        if (notFound <= 10) errors.push(`Código no encontrado: ${code}`);
      }
    } catch(err) {
      errors.push(`Error en código ${code}: ${err.message}`);
    }
  }

  res.json({ success: true, updated, notFound, total: rows.length, errors });
}));

// ── LISTAS DE PRECIOS ─────────────────────────────────────────
app.get('/api/price-lists', asyncHandler(async (req, res) => {
  const { rows } = await query(`
    SELECT pl.*,
      COUNT(pli.id) as item_count,
      array_agg(DISTINCT l.name) FILTER (WHERE l.name IS NOT NULL) as assigned_to
    FROM price_lists pl
    LEFT JOIN price_list_items pli ON pli.price_list_id = pl.id
    LEFT JOIN price_list_assignments pla ON pla.price_list_id = pl.id
    LEFT JOIN leads l ON l.id = pla.lead_id
    GROUP BY pl.id ORDER BY pl.created_at DESC`);
  res.json(rows);
}));

app.post('/api/price-lists', asyncHandler(async (req, res) => {
  const { name, description, discount_pct } = req.body;
  if (!name) return res.status(400).json({ error: 'Nombre requerido' });
  const { rows: [pl] } = await query(
    `INSERT INTO price_lists (id, name, description, discount_pct)
     VALUES ($1,$2,$3,$4) RETURNING *`,
    [require('crypto').randomUUID(), name, description||null, parseFloat(discount_pct)||0]
  );
  res.status(201).json(pl);
}));

app.put('/api/price-lists/:id', asyncHandler(async (req, res) => {
  const { name, description, discount_pct, active } = req.body;
  const { rows: [pl] } = await query(
    `UPDATE price_lists SET name=$1, description=$2, discount_pct=$3, active=$4, updated_at=NOW()
     WHERE id=$5 RETURNING *`,
    [name, description||null, parseFloat(discount_pct)||0, active !== false, req.params.id]
  );
  res.json(pl);
}));

app.delete('/api/price-lists/:id', asyncHandler(async (req, res) => {
  await query('DELETE FROM price_lists WHERE id=$1', [req.params.id]);
  res.json({ success: true });
}));

// Items de una lista
app.get('/api/price-lists/:id/items', asyncHandler(async (req, res) => {
  const { rows } = await query(
    `SELECT pli.*, p.photo_url, p.brand,
      pv.field_value as medida
     FROM price_list_items pli
     LEFT JOIN products p ON p.id = pli.product_id
     LEFT JOIN product_values pv ON pv.product_id = pli.product_id AND pv.field_key = 'medida'
     WHERE pli.price_list_id = $1 ORDER BY pli.created_at ASC`,
    [req.params.id]
  );
  res.json(rows);
}));

app.post('/api/price-lists/:id/items', asyncHandler(async (req, res) => {
  const { product_id, product_name, base_price, discount_pct } = req.body;
  // Si no se especifica discount_pct, usar el de la lista
  const { rows: [pl] } = await query('SELECT discount_pct FROM price_lists WHERE id=$1', [req.params.id]);
  const disc = discount_pct !== undefined && discount_pct !== null ? parseFloat(discount_pct) : parseFloat(pl?.discount_pct || 0);
  const { rows: [item] } = await query(
    `INSERT INTO price_list_items (id, price_list_id, product_id, product_name, base_price, discount_pct)
     VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
    [require('crypto').randomUUID(), req.params.id, product_id||null, product_name, parseFloat(base_price)||0, disc]
  );
  res.status(201).json(item);
}));

app.put('/api/price-lists/:id/items/:itemId', asyncHandler(async (req, res) => {
  const { product_name, base_price, discount_pct, price_mode } = req.body;
  const { rows: [item] } = await query(
    `UPDATE price_list_items SET product_name=$1, base_price=$2, discount_pct=$3, price_mode=$4
     WHERE id=$5 AND price_list_id=$6 RETURNING *`,
    [product_name, parseFloat(base_price)||0, parseFloat(discount_pct)||0,
     price_mode||'descuento', req.params.itemId, req.params.id]
  );
  res.json(item);
}));

app.delete('/api/price-lists/:id/items/:itemId', asyncHandler(async (req, res) => {
  await query('DELETE FROM price_list_items WHERE id=$1 AND price_list_id=$2', [req.params.itemId, req.params.id]);
  res.json({ success: true });
}));

// Asignaciones
app.get('/api/price-lists/:id/assignments', asyncHandler(async (req, res) => {
  const { rows } = await query(
    `SELECT pla.*, l.name as lead_name, l.company, l.email
     FROM price_list_assignments pla
     JOIN leads l ON l.id = pla.lead_id
     WHERE pla.price_list_id = $1`,
    [req.params.id]
  );
  res.json(rows);
}));

app.post('/api/price-lists/:id/assignments', asyncHandler(async (req, res) => {
  const { lead_id } = req.body;
  const { rows: [a] } = await query(
    `INSERT INTO price_list_assignments (id, price_list_id, lead_id)
     VALUES ($1,$2,$3) ON CONFLICT DO NOTHING RETURNING *`,
    [require('crypto').randomUUID(), req.params.id, lead_id]
  );
  res.status(201).json(a || {});
}));

app.delete('/api/price-lists/:id/assignments/:leadId', asyncHandler(async (req, res) => {
  await query('DELETE FROM price_list_assignments WHERE price_list_id=$1 AND lead_id=$2',
    [req.params.id, req.params.leadId]);
  res.json({ success: true });
}));

// Importación masiva de precios base por Excel
const uploadPriceList = multer2({ storage: multer2.memoryStorage(), limits: { fileSize: 5*1024*1024 } });
app.post('/api/price-lists/:id/import', uploadPriceList.single('file'), asyncHandler(async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se subió archivo' });
  const XLSX = require('xlsx');
  const rows = XLSX.utils.sheet_to_json(
    XLSX.read(req.file.buffer, { type:'buffer' }).Sheets[XLSX.read(req.file.buffer,{type:'buffer'}).SheetNames[0]],
    { defval: '' }
  );
  const { rows: [pl] } = await query('SELECT discount_pct FROM price_lists WHERE id=$1', [req.params.id]);
  let created=0, updated=0, errors=[];
  for (const row of rows) {
    const code  = String(row.codigo_proveedor || '').trim();
    const sku   = String(row.codigo_sku || '').trim();
    const price = parseFloat(row.precio_lista || 0);
    if (!code && !sku) continue;
    const searchCode = code || sku;
    const searchField = sku && !code ? 'codigo_sku' : 'codigo_proveedor';
    if (!price) { errors.push('Sin precio para código: ' + code); continue; }
    const name  = code; // nombre temporal, se reemplaza con el del catálogo
    // Buscar producto en catálogo por código o nombre
    let product_id = null;
    let product_name = name;
    if (searchCode) {
      const { rows: prods } = await query(
        `SELECT p.id, p.name FROM products p
         JOIN product_values pv ON pv.product_id=p.id
         WHERE pv.field_key=$1 AND pv.field_value=$2 LIMIT 1`, [searchField, searchCode]);
      if (prods.length > 0) { product_id = prods[0].id; product_name = prods[0].name; }
    }
    // Verificar si ya existe en la lista
    const { rows: existing } = await query(
      `SELECT id FROM price_list_items WHERE price_list_id=$1 AND (product_id=$2 OR product_name=$3)`,
      [req.params.id, product_id, product_name]
    );
    if (existing.length > 0) {
      await query(
        `UPDATE price_list_items SET base_price=$1, discount_pct=$2 WHERE id=$3`,
        [price, disc, existing[0].id]
      );
      updated++;
    } else {
      await query(
        `INSERT INTO price_list_items (id, price_list_id, product_id, product_name, base_price, discount_pct)
         VALUES ($1,$2,$3,$4,$5,$6)`,
        [require('crypto').randomUUID(), req.params.id, product_id, product_name, price, disc]
      );
      created++;
    }
  }
  res.json({ success:true, created, updated, total:rows.length, errors });
}));

// Template Excel para lista de precios
app.get('/api/price-lists/:id/template', asyncHandler(async (req, res) => {
  const XLSX = require('xlsx');
  const { rows: [pl] } = await query('SELECT * FROM price_lists WHERE id=$1', [req.params.id]);

  // Obtener todos los productos con sus campos
  const { rows: products } = await query(
    `SELECT p.name, p.price_normal,
      MAX(CASE WHEN pv.field_key = 'codigo_sku'       THEN pv.field_value END) as codigo_sku,
      MAX(CASE WHEN pv.field_key = 'codigo_proveedor' THEN pv.field_value END) as codigo_proveedor
     FROM products p
     LEFT JOIN product_values pv ON pv.product_id = p.id
     WHERE p.active = true
     GROUP BY p.id, p.name, p.price_normal
     ORDER BY MAX(CASE WHEN pv.field_key = 'codigo_sku' THEN pv.field_value::int END) ASC`
  );

  const data = products.map(p => ({
    codigo_sku:        p.codigo_sku || '',
    codigo_proveedor:  p.codigo_proveedor || '',
    descripcion:       p.name,
    precio_lista:      0,
  }));

  const ws = XLSX.utils.json_to_sheet(data, { header: ['codigo_sku', 'codigo_proveedor', 'descripcion', 'precio_lista'] });
  ws['!cols'] = [{ wch:12 }, { wch:25 }, { wch:55 }, { wch:15 }];

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Lista de Precios');
  const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="lista_${pl?.name||'precios'}.xlsx"`);
  res.send(buffer);
}));

// ── EXPORTAR MAPEO DE CÓDIGOS ────────────────────────────────
app.post('/api/price-lists/export-mapping', asyncHandler(async (req, res) => {
  const { approved_items } = req.body;
  const XLSX = require('xlsx');

  const data = approved_items.map(item => ({
    codigo_proveedor:   item.codigo_proveedor || '',
    descripcion_origen: item.descripcion || '',
    codigo_sku:         item.codigo_sku || '',
    codigo_interno:     item.codigo_interno || '',
    nombre_sistema:     item.product_name || '',
    precio_lista:       item.precio_lista || 0,
  }));

  const ws = XLSX.utils.json_to_sheet(data, {
    header: ['codigo_proveedor','descripcion_origen','codigo_sku','codigo_interno','nombre_sistema','precio_lista']
  });
  ws['!cols'] = [{wch:20},{wch:50},{wch:12},{wch:25},{wch:50},{wch:15}];

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Mapeo');
  const buffer = XLSX.write(wb, { type:'buffer', bookType:'xlsx' });
  res.setHeader('Content-Type','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition','attachment; filename="mapeo_codigos.xlsx"');
  res.send(buffer);
}));

// ── MATCH IA PARA LISTA DE PRECIOS ───────────────────────────
app.post('/api/price-lists/:id/match', uploadExcel.single('file'), asyncHandler(async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se subió archivo' });

  const XLSX = require('xlsx');
  const rows = XLSX.utils.sheet_to_json(
    XLSX.read(req.file.buffer, { type:'buffer' }).Sheets[
      XLSX.read(req.file.buffer,{type:'buffer'}).SheetNames[0]
    ],
    { defval: '' }
  );

  if (!rows.length) return res.status(400).json({ error: 'Archivo vacío' });

  // Cargar catálogo completo para el matching
  const { rows: catalog } = await query(
    `SELECT p.id, p.name, p.brand, p.price_normal,
      MAX(CASE WHEN pv.field_key='codigo_sku'       THEN pv.field_value END) as codigo_sku,
      MAX(CASE WHEN pv.field_key='codigo_proveedor' THEN pv.field_value END) as codigo_proveedor,
      MAX(CASE WHEN pv.field_key='medida'           THEN pv.field_value END) as medida
     FROM products p
     LEFT JOIN product_values pv ON pv.product_id = p.id
     WHERE p.active = true
     GROUP BY p.id, p.name, p.brand, p.price_normal`
  );

  const results = [];

  for (const row of rows) {
    const sku         = String(row.codigo_sku || '').trim();
    const codProv     = String(row.codigo_proveedor || row['Código'] || row['Codigo'] || row['codigo'] || '').trim();
    const desc        = String(row.descripcion || row['Descripción'] || row['Descripcion'] || row.producto || row.nombre || '').trim();
    const marca       = String(row.marca || row['Marca'] || '').trim();
    const precioLista = parseFloat(row.precio_lista || row['Precio de Lista'] || row['Precio Lista'] || row['precio'] || 0);
    const precioFinal = parseFloat(row.precio_final || row['Precio Final'] || row['precio_final'] || 0);

    if (!precioLista) continue;

    let matchedProduct = null;
    let matchType      = null;
    let confidence     = 0;

    // 1. Match exacto por codigo_sku
    if (sku) {
      const found = catalog.find(p => p.codigo_sku === sku);
      if (found) { matchedProduct = found; matchType = 'codigo_sku'; confidence = 100; }
    }

    // 2. Match exacto por codigo_proveedor
    if (!matchedProduct && codProv) {
      const found = catalog.find(p => p.codigo_proveedor === codProv);
      if (found) { matchedProduct = found; matchType = 'codigo_proveedor'; confidence = 100; }
    }

    // 3. Match por IA (Claude) usando descripción
    if (!matchedProduct && desc) {
      try {
        // Preparar lista reducida para Claude (max 50 candidatos por similaridad básica)
        const words = desc.toLowerCase().split(/\s+/).filter(w => w.length > 3);
        let candidates = catalog.filter(p => {
          const pname = p.name.toLowerCase();
          return words.some(w => pname.includes(w));
        }).slice(0, 30);

        if (candidates.length === 0) candidates = catalog.slice(0, 20);

        const catalogSample = candidates.map(p =>
          `ID:${p.id}|SKU:${p.codigo_sku||''}|${p.brand} ${p.name} ${p.medida||''}`
        ).join('\n');

        const Anthropic = require('@anthropic-ai/sdk');
        const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
        const msg = await Promise.race([
          anthropic.messages.create({
            model: 'claude-haiku-4-5-20251001',
            max_tokens: 100,
            messages: [{
              role: 'user',
              content: `Eres un experto en neumáticos. Dado este producto del proveedor:
"${desc}"

Encuentra el mejor match en este catálogo:
${catalogSample}

Responde SOLO con este JSON (sin explicaciones):
{"id":"UUID_del_producto","confidence":85,"reason":"razón breve"}

Si no hay match razonable responde: {"id":null,"confidence":0,"reason":"sin match"}`
            }]
          }),
          new Promise((_,reject) => setTimeout(() => reject(new Error('timeout')), 5000))
        ]);

        const text = msg.content[0].text.trim();
        const parsed = JSON.parse(text.replace(/```json|```/g,'').trim());
        if (parsed.id) {
          matchedProduct = catalog.find(p => p.id === parsed.id);
          matchType = 'ia_descripcion';
          confidence = parsed.confidence || 0;
        }
      } catch(e) {
        console.error('Error IA match:', e.message);
      }
    }

    results.push({
      row_data: { sku, codigo_proveedor: codProv, descripcion: desc, precio_lista: precioLista, precio_final: precioFinal },
      matched_product: matchedProduct ? {
        id: matchedProduct.id,
        name: matchedProduct.name,
        brand: matchedProduct.brand,
        medida: matchedProduct.medida,
        codigo_sku: matchedProduct.codigo_sku,
        price_normal: matchedProduct.price_normal,
      } : null,
      match_type: matchType,
      confidence,
      approved: confidence === 100, // auto-approve exact matches but user still reviews
    });
  }

  res.json({ results, total: results.length });
}));

// Guardar items aprobados
app.post('/api/price-lists/:id/match/approve', asyncHandler(async (req, res) => {
  const { approved_items } = req.body;
  const { rows: [pl] } = await query('SELECT discount_pct FROM price_lists WHERE id=$1', [req.params.id]);
  let saved = 0;

  for (const item of approved_items) {
    const { product_id, product_name, precio_lista, precio_final } = item;
    const { rows: existing } = await query(
      `SELECT id FROM price_list_items WHERE price_list_id=$1 AND (product_id=$2 OR product_name=$3)`,
      [req.params.id, product_id, product_name]
    );
    // Precio Lista = referencia del proveedor
    // Precio Final = precio acordado con el cliente (precio fijo)
    // Si existe Precio Final → price_mode = precio_fijo, base_price = precio_final
    // Si solo existe Precio Lista → price_mode = descuento, base_price = precio_lista
    const hasFinalPrice = precio_final && precio_final > 0
    const priceMode     = hasFinalPrice ? 'precio_fijo' : 'descuento'
    const basePrice     = hasFinalPrice ? precio_final : precio_lista
    const discPct       = hasFinalPrice ? 0 : parseFloat(pl?.discount_pct||0)

    if (existing.length > 0) {
      await query(
        `UPDATE price_list_items SET base_price=$1, price_mode=$2, discount_pct=$3 WHERE id=$4`,
        [basePrice, priceMode, discPct, existing[0].id]
      );
    } else {
      await query(
        `INSERT INTO price_list_items (id, price_list_id, product_id, product_name, base_price, discount_pct, price_mode)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [require('crypto').randomUUID(), req.params.id, product_id, product_name,
         basePrice, discPct, priceMode]
      );
    }
    saved++;
  }
  res.json({ success: true, saved });
}));

// ── MATCH DE CATÁLOGO ─────────────────────────────────────────
const uploadMatchFile = multer2({ storage: multer2.memoryStorage(), limits: { fileSize: 10*1024*1024 } });

// Subir Excel y procesar match por marca
app.post('/api/catalog-match', uploadMatchFile.single('file'), asyncHandler(async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se subió archivo' });
  const { brand } = req.body;
  if (!brand) return res.status(400).json({ error: 'Marca requerida' });

  const XLSX = require('xlsx');
  const rows = XLSX.utils.sheet_to_json(
    XLSX.read(req.file.buffer, { type:'buffer' }).Sheets[
      XLSX.read(req.file.buffer,{type:'buffer'}).SheetNames[0]
    ],
    { defval: '' }
  );

  if (!rows.length) return res.status(400).json({ error: 'Archivo vacío' });

  // Crear job
  const { rows: [job] } = await query(
    `INSERT INTO catalog_match_jobs (id, brand, file_name, total_rows)
     VALUES ($1,$2,$3,$4) RETURNING *`,
    [require('crypto').randomUUID(), brand, req.file.originalname, rows.length]
  );

  // Cargar catálogo de esa marca
  const { rows: catalog } = await query(
    `SELECT p.id, p.name, p.brand,
      MAX(CASE WHEN pv.field_key='codigo_sku'       THEN pv.field_value END) as codigo_sku,
      MAX(CASE WHEN pv.field_key='codigo_proveedor' THEN pv.field_value END) as codigo_proveedor,
      MAX(CASE WHEN pv.field_key='medida'           THEN pv.field_value END) as medida,
      MAX(CASE WHEN pv.field_key='modelo_neumatico' THEN pv.field_value END) as modelo
     FROM products p
     LEFT JOIN product_values pv ON pv.product_id = p.id
     WHERE p.active = true AND UPPER(p.brand) = UPPER($1)
     GROUP BY p.id, p.name, p.brand`,
    [brand]
  );

  // Índices rápidos
  const byCodProv = {};
  const byName    = {};
  catalog.forEach(p => {
    if (p.codigo_proveedor) byCodProv[p.codigo_proveedor.toLowerCase().trim()] = p;
    byName[p.name.toLowerCase().trim()] = p;
  });

  let matched = 0, unmatched = 0;

  for (const row of rows) {
    const rawCode  = String(row['Código']    || row['codigo']    || row['Code']  || row.codigo_proveedor || '').trim();
    const rawDesc  = String(row['Descripción']|| row['Descripcion']|| row['Description']|| row['Descr.'] || row['Descr'] || row.descripcion || row.nombre || '').trim();
    const rawMed   = String(row['Medida']    || row['medida']    || row['Size']  || '').trim();
    const rawDiam  = String(row['Diámetro']  || row['Diametro']  || row['diametro'] || row['Aro'] || '').trim();
    const rawMod   = String(row['Modelo']    || row['modelo']    || row['Model'] || '').trim();
    const rawMarca = String(row['Marca']     || row['marca']     || row['Brand'] || brand).trim();
    const rawPrice = parseFloat(row['Precio']|| row['precio']    || row['Price'] || row['Precio Lista'] || 0);

    if (!rawDesc && !rawCode) continue;

    let matchedProduct = null;
    let matchType = null;
    let confidence = 0;

    // 1. Match exacto por código proveedor
    if (rawCode && byCodProv[rawCode.toLowerCase()]) {
      matchedProduct = byCodProv[rawCode.toLowerCase()];
      matchType = 'codigo'; confidence = 100;
    }
    // 2. Match por nombre exacto
    else if (rawDesc && byName[rawDesc.toLowerCase()]) {
      matchedProduct = byName[rawDesc.toLowerCase()];
      matchType = 'nombre'; confidence = 95;
    }
    // 3. Match parcial por descripción
    else if (rawDesc) {
      const words = rawDesc.toLowerCase().split(/\s+/).filter(w => w.length > 3);
      let bestMatch = null, bestScore = 0;
      for (const p of catalog) {
        const pname = p.name.toLowerCase();
        const score = words.filter(w => pname.includes(w)).length / words.length;
        if (score > bestScore && score >= 0.6) { bestScore = score; bestMatch = p; }
      }
      if (bestMatch) { matchedProduct = bestMatch; matchType = 'parcial'; confidence = Math.round(bestScore * 80); }
    }

    const status = matchedProduct ? 'matched' : 'unmatched';
    if (matchedProduct) matched++; else unmatched++;

    await query(
      `INSERT INTO catalog_match_items (id, job_id, raw_code, raw_description, raw_medida, raw_diametro, raw_modelo, raw_marca, raw_price, matched_product_id, match_type, confidence, status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
      [require('crypto').randomUUID(), job.id, rawCode, rawDesc, rawMed, rawDiam, rawMod, rawMarca, rawPrice,
       matchedProduct?.id||null, matchType, confidence, status]
    );
  }

  // Actualizar job
  await query(
    `UPDATE catalog_match_jobs SET status='done', matched=$1, unmatched=$2 WHERE id=$3`,
    [matched, unmatched, job.id]
  );

  res.json({ job_id: job.id, matched, unmatched, total: rows.length });
}));

// Listar jobs
app.get('/api/catalog-match', asyncHandler(async (req, res) => {
  const { rows } = await query('SELECT * FROM catalog_match_jobs ORDER BY created_at DESC LIMIT 20');
  res.json(rows);
}));

// Items de un job
app.get('/api/catalog-match/:jobId/items', asyncHandler(async (req, res) => {
  const { status, page=1, limit=50 } = req.query;
  let where = 'WHERE cmi.job_id=$1';
  const params = [req.params.jobId];
  if (status) { where += ' AND cmi.status=$2'; params.push(status); }
  const offset = (parseInt(page)-1)*parseInt(limit);

  const [data, count] = await Promise.all([
    query(`SELECT cmi.*, p.name as matched_name, p.brand as matched_brand,
      pv_med.field_value as matched_medida
      FROM catalog_match_items cmi
      LEFT JOIN products p ON p.id = cmi.matched_product_id
      LEFT JOIN product_values pv_med ON pv_med.product_id = p.id AND pv_med.field_key='medida'
      ${where} ORDER BY cmi.status, cmi.raw_description
      LIMIT $${params.length+1} OFFSET $${params.length+2}`,
      [...params, parseInt(limit), offset]),
    query(`SELECT COUNT(*) as total FROM catalog_match_items cmi ${where}`, params)
  ]);

  res.json({ items: data.rows, total: parseInt(count.rows[0].total), page: parseInt(page), pages: Math.ceil(count.rows[0].total/parseInt(limit)) });
}));

// Crear producto desde item no matcheado
app.post('/api/catalog-match/:jobId/items/:itemId/create', asyncHandler(async (req, res) => {
  const { name, brand, category, price_normal, price_offer, stock, photo_url, custom_fields } = req.body;
  const productId = require('crypto').randomUUID();

  const { rows: [product] } = await query(
    `INSERT INTO products (id,name,brand,category,price_normal,price_offer,stock,photo_url,active,unit)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,true,'unidad') RETURNING *`,
    [productId, name, brand, category||'Neumático', parseFloat(price_normal)||0,
     price_offer?parseFloat(price_offer):null, parseInt(stock)||0, photo_url||null]
  );

  // Guardar campos personalizados
  if (custom_fields) {
    for (const [key, value] of Object.entries(custom_fields)) {
      if (value) {
        await query(
          `INSERT INTO product_values (id, product_id, field_key, field_value)
           VALUES ($1,$2,$3,$4) ON CONFLICT (product_id, field_key) DO UPDATE SET field_value=$4`,
          [require('crypto').randomUUID(), productId, key, String(value)]
        );
      }
    }
  }

  // Asignar SKU secuencial
  const { rows: [maxSku] } = await query(`SELECT MAX(field_value::int) as max FROM product_values WHERE field_key='codigo_sku' AND field_value ~ '^[0-9]+$'`);
  const newSku = (parseInt(maxSku?.max||0) + 1).toString();
  await query(
    `INSERT INTO product_values (id, product_id, field_key, field_value) VALUES ($1,$2,'codigo_sku',$3)`,
    [require('crypto').randomUUID(), productId, newSku]
  );

  // Marcar item como creado
  await query(
    `UPDATE catalog_match_items SET status='created', created_product_id=$1 WHERE id=$2`,
    [productId, req.params.itemId]
  );

  // Actualizar job
  await query(
    `UPDATE catalog_match_jobs SET unmatched = unmatched-1, matched = matched+1 WHERE id=$1`,
    [req.params.jobId]
  );

  res.status(201).json(product);
}));

// Asignar match manual
app.patch('/api/catalog-match/:jobId/items/:itemId/match', asyncHandler(async (req, res) => {
  const { product_id } = req.body;
  await query(
    `UPDATE catalog_match_items SET matched_product_id=$1, match_type='manual', confidence=100, status='matched' WHERE id=$2`,
    [product_id, req.params.itemId]
  );
  await query(
    `UPDATE catalog_match_jobs SET unmatched=unmatched-1, matched=matched+1 WHERE id=$1`,
    [req.params.jobId]
  );
  res.json({ success: true });
}));

// ── FAMILIAS Y MODELOS POR MARCA ─────────────────────────────
app.get('/api/catalog/families', asyncHandler(async (req, res) => {
  const { brand } = req.query;
  let q = `SELECT brand, familia, modelo, productos FROM catalog_families WHERE familia IS NOT NULL`;
  const params = [];
  if (brand) { q += ` AND UPPER(brand) = UPPER($1)`; params.push(brand); }
  q += ` ORDER BY brand, familia, modelo`;
  const { rows } = await query(q, params);
  res.json(rows);
}));

// ── FAMILIAS Y MODELOS ────────────────────────────────────────
app.get('/api/families', asyncHandler(async (req, res) => {
  const { brand, search } = req.query;
  let where = 'WHERE 1=1';
  const params = [];
  let i = 1;
  if (brand)  { where += ` AND UPPER(pf.brand) = UPPER($${i})`; params.push(brand); i++; }
  if (search) { where += ` AND (pf.familia ILIKE $${i} OR pf.modelo ILIKE $${i} OR pf.brand ILIKE $${i})`; params.push('%'+search+'%'); i++; }

  // Agrupar por brand+familia, agregar modelos como array
  const { rows } = await query(`
    SELECT 
      pf.brand,
      pf.familia,
      pf.modelo,
      (array_agg(pf.id ORDER BY pf.created_at ASC))[1] as id,
      array_agg(DISTINCT pf.modelo) FILTER (WHERE pf.modelo IS NOT NULL) as modelos,
      array_agg(DISTINCT pf.id) as ids,
      MAX(pf.description) as description,
      MAX(pf.caracteristicas) as caracteristicas,
      MAX(pf.beneficios) as beneficios,
      MAX(pf.etiquetas::text)::text[] as etiquetas,
      SUM(DISTINCT COALESCE((
        SELECT COUNT(DISTINCT p.id) FROM products p
        WHERE UPPER(p.brand)=UPPER(pf.brand)
        AND EXISTS (SELECT 1 FROM product_values pv WHERE pv.product_id=p.id AND pv.field_key='familia' AND pv.field_value=pf.familia)
      ),0)) as product_count,
      json_agg(DISTINCT jsonb_build_object('id',fp.id,'type',fp.photo_type,'url',fp.photo_url,'ord',fp.ord))
        FILTER (WHERE fp.id IS NOT NULL) as photos
    FROM product_families pf
    LEFT JOIN family_photos fp ON fp.family_id = pf.id
    ${where}
    GROUP BY pf.brand, pf.familia, pf.modelo
    ORDER BY pf.brand, pf.familia, pf.modelo`, params);
  res.json(rows);
}));

app.get('/api/families/:id', asyncHandler(async (req, res) => {
  const { rows: [fam] } = await query(
    `SELECT pf.*, 
      json_agg(DISTINCT jsonb_build_object('id',fp.id,'type',fp.photo_type,'url',fp.photo_url,'ord',fp.ord))
        FILTER (WHERE fp.id IS NOT NULL) as photos
     FROM product_families pf
     LEFT JOIN family_photos fp ON fp.family_id = pf.id
     WHERE pf.id=$1 GROUP BY pf.id`, [req.params.id]);
  if (!fam) return res.status(404).json({ error: 'No encontrada' });
  res.json(fam);
}));

app.post('/api/families', asyncHandler(async (req, res) => {
  const { brand, familia, modelo, description, caracteristicas, beneficios, etiquetas } = req.body;
  if (!brand || !familia) return res.status(400).json({ error: 'Marca y familia requeridas' });
  const { rows: [fam] } = await query(
    `INSERT INTO product_families (id,brand,familia,modelo,description,caracteristicas,beneficios,etiquetas)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (brand,familia,modelo) DO UPDATE
     SET description=$5,caracteristicas=$6,beneficios=$7,etiquetas=$8,updated_at=NOW()
     RETURNING *`,
    [require('crypto').randomUUID(), brand, familia, modelo||null, description||null,
     caracteristicas||null, beneficios||null, etiquetas||null]
  );
  res.status(201).json(fam);
}));

app.put('/api/families/:id', asyncHandler(async (req, res) => {
  const { brand, familia, modelo, description, caracteristicas, beneficios, etiquetas, active } = req.body;
  const { rows: [fam] } = await query(
    `UPDATE product_families SET brand=$1,familia=$2,modelo=$3,description=$4,
     caracteristicas=$5,beneficios=$6,etiquetas=$7,active=$8,updated_at=NOW()
     WHERE id=$9 RETURNING *`,
    [brand, familia, modelo||null, description||null, caracteristicas||null,
     beneficios||null, etiquetas||null, active!==false, req.params.id]
  );
  res.json(fam);
}));

app.delete('/api/families/:id', asyncHandler(async (req, res) => {
  await query('DELETE FROM product_families WHERE id=$1', [req.params.id]);
  res.json({ success: true });
}));

// Fotos de familia
const uploadFamilyPhoto = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, uploadsProductsDir),
    filename: (req, file, cb) => {
      const ext = require('path').extname(file.originalname).toLowerCase();
      cb(null, 'family-' + Date.now() + '-' + Math.random().toString(36).slice(2) + ext);
    }
  }),
  limits: { fileSize: 3*1024*1024 },
  fileFilter: (req, file, cb) => {
    const allowed = ['.jpg','.jpeg','.png','.webp'];
    cb(null, allowed.includes(require('path').extname(file.originalname).toLowerCase()));
  }
});

app.post('/api/families/:id/photos', uploadFamilyPhoto.single('photo'), asyncHandler(async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se subió foto' });
  const { photo_type, ord } = req.body;
  const url = `${BASE_URL}/uploads/products/${req.file.filename}`;
  // Reemplazar si ya existe ese tipo
  await query('DELETE FROM family_photos WHERE family_id=$1 AND photo_type=$2', [req.params.id, photo_type]);
  const { rows: [photo] } = await query(
    `INSERT INTO family_photos (id,family_id,photo_type,photo_url,ord) VALUES ($1,$2,$3,$4,$5) RETURNING *`,
    [require('crypto').randomUUID(), req.params.id, photo_type||'extra', url, parseInt(ord)||0]
  );
  res.status(201).json(photo);
}));

app.delete('/api/families/:id/photos/:photoId', asyncHandler(async (req, res) => {
  await query('DELETE FROM family_photos WHERE id=$1 AND family_id=$2', [req.params.photoId, req.params.id]);
  res.json({ success: true });
}));

// ── NORMALIZACIÓN DE CATÁLOGO ─────────────────────────────────
app.get('/api/catalog/normalize/duplicates', asyncHandler(async (req, res) => {
  const { brand, field_key = 'modelo_neumatico' } = req.query;

  let where = `WHERE pv.field_key = $1 AND pv.field_value != ''`;
  const params = [field_key];
  if (brand) { where += ` AND UPPER(p.brand) = UPPER($2)`; params.push(brand); }

  // Detectar duplicados por marca - agrupar por brand + normalized
  const groupByBrand = brand ? '' : ', p.brand';
  const selectBrand  = brand ? `'${brand}'` : 'p.brand';

  const { rows } = await query(`
    SELECT 
      ${brand ? `'${brand}' as brand,` : 'p.brand,'}
      UPPER(REGEXP_REPLACE(pv.field_value, '[^A-Z0-9]', '', 'g')) as normalized,
      array_agg(DISTINCT pv.field_value ORDER BY pv.field_value) as variants,
      COUNT(DISTINCT pv.field_value) as variant_count,
      COUNT(DISTINCT p.id) as product_count
    FROM product_values pv
    JOIN products p ON p.id = pv.product_id AND p.active = true
    ${where}
    GROUP BY ${brand ? '' : 'p.brand, '}UPPER(REGEXP_REPLACE(pv.field_value, '[^A-Z0-9]', '', 'g'))
    HAVING COUNT(DISTINCT pv.field_value) > 1
    ORDER BY ${brand ? '' : 'p.brand, '}COUNT(DISTINCT pv.field_value) DESC, COUNT(DISTINCT p.id) DESC
  `, params);

  res.json(rows);
}));

// Fusionar variantes — reemplazar todas por el valor canónico
app.post('/api/catalog/normalize/merge', asyncHandler(async (req, res) => {
  const { field_key, canonical, variants, brand } = req.body;
  if (!field_key || !canonical || !variants?.length) 
    return res.status(400).json({ error: 'Parámetros requeridos' });

  let updated = 0;
  for (const variant of variants) {
    if (variant === canonical) continue;
    let q = `UPDATE product_values SET field_value = $1 
             WHERE field_key = $2 AND field_value = $3`;
    const params = [canonical, field_key, variant];
    if (brand) { 
      q += ` AND product_id IN (SELECT id FROM products WHERE UPPER(brand) = UPPER($4))`;
      params.push(brand);
    }
    const { rowCount } = await query(q, params);
    updated += rowCount;
  }

  // Limpiar familias duplicadas si es familia o modelo
  if (field_key === 'familia' || field_key === 'modelo_neumatico') {
    await query(`
      DELETE FROM product_families pf1
      WHERE EXISTS (
        SELECT 1 FROM product_families pf2
        WHERE pf2.id != pf1.id
        AND UPPER(pf2.brand) = UPPER(pf1.brand)
        AND pf2.familia = pf1.familia
        AND (pf2.modelo = pf1.modelo OR (pf2.modelo IS NULL AND pf1.modelo IS NULL))
        AND pf2.created_at < pf1.created_at
      )
    `);
  }

  res.json({ success: true, updated });
}));

// Normalizar mayúsculas/minúsculas de un campo
app.post('/api/catalog/normalize/case', asyncHandler(async (req, res) => {
  const { field_key, brand, mode = 'initcap' } = req.body;
  
  let caseFunc = mode === 'upper' ? 'UPPER' : mode === 'lower' ? 'LOWER' : 'INITCAP';
  let q = `UPDATE product_values SET field_value = ${caseFunc}(field_value) WHERE field_key = $1`;
  const params = [field_key];
  if (brand) { q += ` AND product_id IN (SELECT id FROM products WHERE UPPER(brand) = UPPER($2))`; params.push(brand); }
  
  const { rowCount } = await query(q, params);
  res.json({ success: true, updated: rowCount });
}));

// ── CÓDIGOS OEM ───────────────────────────────────────────────
app.get('/api/oem-codes', asyncHandler(async (req, res) => {
  const { rows } = await query('SELECT * FROM oem_codes ORDER BY brand_car, code');
  res.json(rows);
}));

app.put('/api/oem-codes/:code', asyncHandler(async (req, res) => {
  const { brand_oem, brand_car, description, logo_url } = req.body;
  const { rows: [oem] } = await query(
    `UPDATE oem_codes SET brand_oem=$1, brand_car=$2, description=$3, logo_url=$4
     WHERE code=$5 RETURNING *`,
    [brand_oem, brand_car, description, logo_url||null, req.params.code]
  );
  res.json(oem);
}));

// Upload logo OEM
const uploadOemLogo = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, uploadsProductsDir),
    filename: (req, file, cb) => {
      const ext = require('path').extname(file.originalname).toLowerCase();
      cb(null, 'oem-' + req.params.code + ext);
    }
  }),
  limits: { fileSize: 2*1024*1024 },
});

app.post('/api/oem-codes/:code/logo', uploadOemLogo.single('logo'), asyncHandler(async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se subió archivo' });
  const url = `${BASE_URL}/uploads/products/oem-${req.params.code}${require('path').extname(req.file.originalname).toLowerCase()}`;
  await query('UPDATE oem_codes SET logo_url=$1 WHERE code=$2', [url, req.params.code]);
  res.json({ url });
}));

// ── MATCH DESDE PDF ───────────────────────────────────────────
const uploadPdfMatch = multer2({ storage: multer2.memoryStorage(), limits: { fileSize: 20*1024*1024 } });

app.post('/api/catalog-match/pdf', uploadPdfMatch.single('file'), asyncHandler(async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se subió archivo' });
  const { brand } = req.body;
  if (!brand) return res.status(400).json({ error: 'Marca requerida' });

  // Extraer texto del PDF
  const pdfParse = require('pdf-parse');
  const pdfData = await pdfParse(req.file.buffer);
  const text = pdfData.text;

  if (!text || text.length < 50) 
    return res.status(400).json({ error: 'No se pudo extraer texto del PDF' });

  // Usar Claude para estructurar los datos
  const Anthropic = require('@anthropic-ai/sdk');
  const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

  // Procesar en chunks si el texto es muy largo
  const maxChunk = 15000;
  const chunks = [];
  for (let i = 0; i < text.length; i += maxChunk) {
    chunks.push(text.slice(i, i + maxChunk));
  }

  let allRows = [];

  for (const chunk of chunks) {
    try {
      const msg = await anthropic.messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 4000,
        messages: [{
          role: 'user',
          content: `Analiza este texto extraído de un catálogo de neumáticos PDF y extrae todos los productos.
Campos disponibles: Codigo, Medida, LI_SS (índice carga/velocidad juntos), Pattern (parte del modelo), Diseño (familia+modelo), Tipo_uso, SW, Origen.

Responde SOLO con un array JSON válido, sin explicaciones ni backticks:
[{"codigo":"...","medida":"...","li_ss":"...","pattern":"...","diseno":"...","tipo_uso":"...","sw":"...","origen":"..."}]

Si un campo no existe, usa string vacío "".
Texto del PDF:
${chunk}`
        }]
      });

      const responseText = msg.content[0].text.trim();
      const cleaned = responseText.replace(/```json|```/g, '').trim();
      const rows = JSON.parse(cleaned);
      if (Array.isArray(rows)) allRows = allRows.concat(rows);
    } catch(e) {
      console.error('Error parsing PDF chunk:', e.message);
    }
  }

  if (!allRows.length) return res.status(400).json({ error: 'No se encontraron productos en el PDF' });

  // Cargar catálogo de esa marca
  const { rows: catalog } = await query(
    `SELECT p.id, p.name, p.brand,
      MAX(CASE WHEN pv.field_key='codigo_sku'       THEN pv.field_value END) as codigo_sku,
      MAX(CASE WHEN pv.field_key='codigo_proveedor' THEN pv.field_value END) as codigo_proveedor,
      MAX(CASE WHEN pv.field_key='medida'           THEN pv.field_value END) as medida,
      MAX(CASE WHEN pv.field_key='modelo_neumatico' THEN pv.field_value END) as modelo,
      MAX(CASE WHEN pv.field_key='familia'          THEN pv.field_value END) as familia
     FROM products p
     LEFT JOIN product_values pv ON pv.product_id = p.id
     WHERE p.active = true AND UPPER(p.brand) = UPPER($1)
     GROUP BY p.id, p.name, p.brand`,
    [brand]
  );

  // Índices para búsqueda rápida
  const byCodProv = {};
  const byMedida  = {};
  catalog.forEach(p => {
    if (p.codigo_proveedor) byCodProv[p.codigo_proveedor.toLowerCase().trim()] = p;
    if (p.medida) {
      if (!byMedida[p.medida.toLowerCase().trim()]) byMedida[p.medida.toLowerCase().trim()] = [];
      byMedida[p.medida.toLowerCase().trim()].push(p);
    }
  });

  // Crear job
  const { rows: [job] } = await query(
    `INSERT INTO catalog_match_jobs (id, brand, file_name, total_rows)
     VALUES ($1,$2,$3,$4) RETURNING *`,
    [require('crypto').randomUUID(), brand, req.file.originalname, allRows.length]
  );

  let matched = 0, unmatched = 0;

  for (const row of allRows) {
    const rawCode  = String(row.codigo   || '').trim();
    const rawMed   = String(row.medida   || '').trim();
    const rawDesc  = String(row.diseno   || row.pattern || '').trim();
    const rawLISS  = String(row.li_ss    || '').trim();
    const rawMod   = String(row.pattern  || '').trim();
    const rawUso   = String(row.tipo_uso || '').trim();
    const rawOrig  = String(row.origen   || '').trim();
    const rawSW    = String(row.sw       || '').trim();

    if (!rawCode && !rawMed && !rawDesc) continue;

    let matchedProduct = null;
    let matchType = null;
    let confidence = 0;

    // 1. Match por código proveedor
    if (rawCode && byCodProv[rawCode.toLowerCase()]) {
      matchedProduct = byCodProv[rawCode.toLowerCase()];
      matchType = 'codigo'; confidence = 100;
    }
    // 2. Match por medida + modelo
    else if (rawMed && byMedida[rawMed.toLowerCase()]) {
      const candidates = byMedida[rawMed.toLowerCase()];
      if (candidates.length === 1) {
        matchedProduct = candidates[0]; matchType = 'medida'; confidence = 85;
      } else if (rawDesc) {
        const desc = rawDesc.toLowerCase();
        const best = candidates.find(c =>
          (c.modelo && desc.includes(c.modelo.toLowerCase())) ||
          (c.familia && desc.includes(c.familia.toLowerCase()))
        );
        if (best) { matchedProduct = best; matchType = 'medida+modelo'; confidence = 90; }
        else { matchedProduct = candidates[0]; matchType = 'medida'; confidence = 70; }
      }
    }

    const status = matchedProduct ? 'matched' : 'unmatched';
    if (matchedProduct) matched++; else unmatched++;

    await query(
      `INSERT INTO catalog_match_items (id, job_id, raw_code, raw_description, raw_medida, raw_diametro, raw_modelo, raw_marca, raw_price, matched_product_id, match_type, confidence, status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
      [require('crypto').randomUUID(), job.id,
       rawCode, rawDesc || rawMod, rawMed, rawLISS, rawMod, brand, 0,
       matchedProduct?.id||null, matchType, confidence, status]
    );
  }

  await query(
    `UPDATE catalog_match_jobs SET status='done', matched=$1, unmatched=$2 WHERE id=$3`,
    [matched, unmatched, job.id]
  );

  res.json({ job_id: job.id, matched, unmatched, total: allRows.length });
}));

// ── TALLERES ──────────────────────────────────────────────────
app.get('/api/workshops', asyncHandler(async (req, res) => {
  const { search, active = 'true' } = req.query;
  let q = `SELECT w.*,
    COUNT(DISTINCT ws.dia) FILTER (WHERE ws.activo=true) as dias_activos,
    COUNT(DISTINCT wp.id) as servicios_precios
    FROM workshops w
    LEFT JOIN workshop_schedules ws ON ws.workshop_id = w.id
    LEFT JOIN workshop_prices wp ON wp.workshop_id = w.id
    WHERE w.active = $1`;
  const params = [active === 'true'];
  if (search) { q += ` AND (w.nombre_comercial ILIKE $2 OR w.comuna ILIKE $2)`; params.push('%'+search+'%'); }
  q += ' GROUP BY w.id ORDER BY w.nombre_comercial';
  const { rows } = await query(q, params);
  res.json(rows);
}));

app.get('/api/workshops/:id', asyncHandler(async (req, res) => {
  const [w, schedules, services, prices] = await Promise.all([
    query('SELECT * FROM workshops WHERE id=$1', [req.params.id]),
    query('SELECT * FROM workshop_schedules WHERE workshop_id=$1 ORDER BY CASE dia WHEN \'lunes\' THEN 1 WHEN \'martes\' THEN 2 WHEN \'miércoles\' THEN 3 WHEN \'jueves\' THEN 4 WHEN \'viernes\' THEN 5 WHEN \'sábado\' THEN 6 WHEN \'domingo\' THEN 7 END', [req.params.id]),
    query('SELECT * FROM workshop_services WHERE workshop_id=$1 ORDER BY nombre', [req.params.id]),
    query('SELECT * FROM workshop_prices WHERE workshop_id=$1 ORDER BY tipo, aro_min', [req.params.id]),
  ]);
  if (!w.rows[0]) return res.status(404).json({ error: 'Taller no encontrado' });
  res.json({ ...w.rows[0], schedules: schedules.rows, services: services.rows, prices: prices.rows });
}));

app.post('/api/workshops', asyncHandler(async (req, res) => {
  const { razon_social, nombre_comercial, rut, encargado_nombre, encargado_email, encargado_phone,
    finanzas_nombre, finanzas_email, finanzas_phone, direccion, comuna, comunas_adicionales,
    latitud, longitud, maps_url, puestos, turnos_por_puesto, aro_min, aro_max,
    instala_runflat, tipos_vehiculo, marcas_neumaticos, todas_marcas } = req.body;
  if (!nombre_comercial) return res.status(400).json({ error: 'Nombre comercial requerido' });
  const { rows: [w] } = await query(
    `INSERT INTO workshops (id,razon_social,nombre_comercial,rut,encargado_nombre,encargado_email,
     encargado_phone,finanzas_nombre,finanzas_email,finanzas_phone,direccion,comuna,
     comunas_adicionales,latitud,longitud,maps_url,puestos,turnos_por_puesto,aro_min,aro_max,
     instala_runflat,tipos_vehiculo,marcas_neumaticos,todas_marcas)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24) RETURNING *`,
    [require('crypto').randomUUID(),razon_social||nombre_comercial,nombre_comercial,rut||null,
     encargado_nombre||null,encargado_email||null,encargado_phone||null,
     finanzas_nombre||null,finanzas_email||null,finanzas_phone||null,
     direccion||null,comuna||null,comunas_adicionales||null,
     latitud||null,longitud||null,maps_url||null,
     parseInt(puestos)||1,parseInt(turnos_por_puesto)||1,
     parseInt(aro_min)||13,parseInt(aro_max)||22,
     instala_runflat||false,tipos_vehiculo||null,
     marcas_neumaticos||null,todas_marcas!==false]
  );
  res.status(201).json(w);
}));

app.put('/api/workshops/:id', asyncHandler(async (req, res) => {
  const { razon_social,nombre_comercial,rut,encargado_nombre,encargado_email,encargado_phone,
    finanzas_nombre,finanzas_email,finanzas_phone,direccion,comuna,comunas_adicionales,
    latitud,longitud,maps_url,puestos,turnos_por_puesto,aro_min,aro_max,
    instala_runflat,tipos_vehiculo,marcas_neumaticos,todas_marcas,active } = req.body;
  const { rows: [w] } = await query(
    `UPDATE workshops SET razon_social=$1,nombre_comercial=$2,rut=$3,encargado_nombre=$4,
     encargado_email=$5,encargado_phone=$6,finanzas_nombre=$7,finanzas_email=$8,finanzas_phone=$9,
     direccion=$10,comuna=$11,comunas_adicionales=$12,latitud=$13,longitud=$14,maps_url=$15,
     puestos=$16,turnos_por_puesto=$17,aro_min=$18,aro_max=$19,instala_runflat=$20,
     tipos_vehiculo=$21,marcas_neumaticos=$22,todas_marcas=$23,active=$24,updated_at=NOW()
     WHERE id=$25 RETURNING *`,
    [razon_social,nombre_comercial,rut||null,encargado_nombre||null,encargado_email||null,
     encargado_phone||null,finanzas_nombre||null,finanzas_email||null,finanzas_phone||null,
     direccion||null,comuna||null,comunas_adicionales||null,latitud||null,longitud||null,
     maps_url||null,parseInt(puestos)||1,parseInt(turnos_por_puesto)||1,
     parseInt(aro_min)||13,parseInt(aro_max)||22,instala_runflat||false,
     tipos_vehiculo||null,marcas_neumaticos||null,todas_marcas!==false,active!==false,req.params.id]
  );
  res.json(w);
}));

// Horarios
app.put('/api/workshops/:id/schedules', asyncHandler(async (req, res) => {
  const { schedules } = req.body;
  console.log('SAVING SCHEDULES for', req.params.id, '- count:', schedules?.length);
  for (const s of schedules) {
    await query(
      `INSERT INTO workshop_schedules (id,workshop_id,dia,activo,hora_inicio,hora_fin,horas)
       VALUES ($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (workshop_id,dia) DO UPDATE SET activo=$4,hora_inicio=$5,hora_fin=$6,horas=$7`,
      [require('crypto').randomUUID(),req.params.id,s.dia,s.activo,s.hora_inicio||'09:00',s.hora_fin||'18:00',s.horas||null]
    );
  }
  res.json({ success: true });
}));

// Servicios
app.post('/api/workshops/:id/services', asyncHandler(async (req, res) => {
  const { nombre, descripcion } = req.body;
  const { rows: [s] } = await query(
    `INSERT INTO workshop_services (id,workshop_id,nombre,descripcion) VALUES ($1,$2,$3,$4) RETURNING *`,
    [require('crypto').randomUUID(),req.params.id,nombre,descripcion||null]
  );
  res.status(201).json(s);
}));

app.delete('/api/workshops/:id/services/:sid', asyncHandler(async (req, res) => {
  await query('DELETE FROM workshop_services WHERE id=$1 AND workshop_id=$2',[req.params.sid,req.params.id]);
  res.json({ success: true });
}));

// Precios
app.post('/api/workshops/:id/prices', asyncHandler(async (req, res) => {
  const { tipo, descripcion, aro_min, aro_max, precio } = req.body;
  const { rows: [p] } = await query(
    `INSERT INTO workshop_prices (id,workshop_id,tipo,descripcion,aro_min,aro_max,precio)
     VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
    [require('crypto').randomUUID(),req.params.id,tipo,descripcion,aro_min||null,aro_max||null,parseFloat(precio)||0]
  );
  res.status(201).json(p);
}));

app.put('/api/workshops/:id/prices/:pid', asyncHandler(async (req, res) => {
  const { tipo,descripcion,aro_min,aro_max,precio } = req.body;
  const { rows: [p] } = await query(
    `UPDATE workshop_prices SET tipo=$1,descripcion=$2,aro_min=$3,aro_max=$4,precio=$5
     WHERE id=$6 AND workshop_id=$7 RETURNING *`,
    [tipo,descripcion,aro_min||null,aro_max||null,parseFloat(precio)||0,req.params.pid,req.params.id]
  );
  res.json(p);
}));

app.delete('/api/workshops/:id/prices/:pid', asyncHandler(async (req, res) => {
  await query('DELETE FROM workshop_prices WHERE id=$1 AND workshop_id=$2',[req.params.pid,req.params.id]);
  res.json({ success: true });
}));

// Agenda
app.get('/api/workshops/:id/appointments', asyncHandler(async (req, res) => {
  const { fecha_inicio, fecha_fin } = req.query;
  let q = `SELECT wa.*, l.name as lead_name, l.phone as lead_phone
    FROM workshop_appointments wa
    LEFT JOIN leads l ON l.id = wa.lead_id
    WHERE wa.workshop_id=$1`;
  const params = [req.params.id];
  if (fecha_inicio) { q += ` AND wa.fecha >= $2`; params.push(fecha_inicio); }
  if (fecha_fin)    { q += ` AND wa.fecha <= $${params.length+1}`; params.push(fecha_fin); }
  q += ' ORDER BY wa.fecha, wa.hora';
  const { rows } = await query(q, params);
  res.json(rows);
}));

app.post('/api/workshops/:id/appointments', asyncHandler(async (req, res) => {
  const { lead_id, fecha, hora, puesto, notas, order_id } = req.body;
  const { rows: [a] } = await query(
    `INSERT INTO workshop_appointments (id,workshop_id,lead_id,fecha,hora,puesto,notas,order_id)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
    [require('crypto').randomUUID(),req.params.id,lead_id||null,fecha,hora,puesto||1,notas||null,order_id||null]
  );
  res.status(201).json(a);
}));

app.patch('/api/workshops/:id/appointments/:aid/status', asyncHandler(async (req, res) => {
  const { status } = req.body;
  const { rows: [a] } = await query(
    `UPDATE workshop_appointments SET status=$1 WHERE id=$2 AND workshop_id=$3 RETURNING *`,
    [status, req.params.aid, req.params.id]
  );
  res.json(a);
}));

// ── PANEL DE ATENCIÓN ─────────────────────────────────────────
// Resumen IA de conversación del cliente
app.post('/api/attention/summary', asyncHandler(async (req, res) => {
  const { lead_id, force = false } = req.body;

  // Verificar si ya tiene resumen de hoy
  const { rows: [lead] } = await query('SELECT ai_summary, ai_summary_date FROM leads WHERE id=$1', [lead_id]);
  const today = new Date().toISOString().slice(0,10);
  if (!force && lead?.ai_summary && lead?.ai_summary_date?.toISOString?.()?.slice(0,10) === today) {
    return res.json(JSON.parse(lead.ai_summary));
  }

  const [leadData, activities, orders] = await Promise.all([
    query('SELECT * FROM leads WHERE id=$1', [lead_id]),
    query(`SELECT direction, content, created_at FROM activities 
           WHERE lead_id=$1 AND channel='WhatsApp' 
           ORDER BY created_at DESC LIMIT 30`, [lead_id]),
    query(`SELECT * FROM orders WHERE lead_id=$1 ORDER BY created_at DESC LIMIT 5`, [lead_id]),
  ]);

  if (!leadData.rows[0]) return res.status(404).json({ error: 'Lead no encontrado' });

  const conversation = activities.rows.reverse().map(a =>
    `${a.direction === 'inbound' ? 'Cliente' : 'Agente'}: ${a.content}`
  ).join('\n');

  const Anthropic = require('@anthropic-ai/sdk');
  const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

  const msg = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 500,
    messages: [{
      role: 'user',
      content: `Eres un asistente de ventas de neumáticos. Analiza esta conversación y responde en JSON:

Conversación:
${conversation}

Órdenes previas: ${orders.rows.length > 0 ? orders.rows.map(o => `${o.marca} ${o.modelo} ${o.medida} x${o.cantidad}`).join(', ') : 'Sin órdenes'}

Responde SOLO con este JSON (sin explicaciones):
{
  "resumen": "2-3 frases sobre qué necesita el cliente",
  "medida": "medida del neumático si se menciona, sino null",
  "marca_preferida": "marca preferida si menciona, sino null", 
  "presupuesto": "presupuesto aproximado si menciona, sino null",
  "urgencia": "alta|media|baja",
  "cantidad": número o null,
  "accion_recomendada": "qué hacer ahora mismo"
}`
    }]
  });

  const text = msg.content[0].text.trim();
  const parsed = JSON.parse(text.replace(/```json|```/g,'').trim());
  // Guardar en BD para caché
  await query('UPDATE leads SET ai_summary=$1, ai_summary_date=NOW() WHERE id=$2', [JSON.stringify(parsed), lead_id]);

  res.json(parsed);
}));

// Disponibilidad de talleres cercanos

app.get('/api/settings/google-key', (req, res) => {
  res.json({ key: process.env.GOOGLE_PLACES_API_KEY || '' });
});

// ── ANÁLISIS DE NEUMÁTICO POR IMAGEN ─────────────────────────
const uploadTireImage = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10*1024*1024 } });

app.post('/api/attention/analyze-tire', uploadTireImage.single('image'), asyncHandler(async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se subió imagen' });

  const Anthropic = require('@anthropic-ai/sdk');
  const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

  const base64 = req.file.buffer.toString('base64');
  const mediaType = req.file.mimetype || 'image/jpeg';

  const msg = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 800,
    messages: [{
      role: 'user',
      content: [
        {
          type: 'image',
          source: { type: 'base64', media_type: mediaType, data: base64 }
        },
        {
          type: 'text',
          text: `Analiza esta imagen de un neumático y extrae toda la información visible.
Responde SOLO con este JSON (sin explicaciones ni backticks):
{
  "medida": "ej: 205/55 R16",
  "ancho": "205",
  "perfil": "55",
  "aro": "16",
  "indice_carga": "91",
  "indice_velocidad": "V",
  "marca": "ej: Bridgestone",
  "modelo": "ej: Turanza T005",
  "runflat": null,
  "xl": false,
  "otros_codigos": [],
  "condicion": "nuevo|usado|desgastado",
  "confianza": 85,
  "notas": "cualquier info adicional visible"
}`
        }
      ]
    }]
  });

  const text = msg.content[0].text.trim();
  const parsed = JSON.parse(text.replace(/```json|```/g,'').trim());

  // Buscar productos en catálogo que coincidan con la medida
  let productos = [];
  if (parsed.medida) {
    const { rows } = await query(`
      SELECT p.id, p.name, p.brand, p.price_normal, p.stock, p.photo_url,
        MAX(CASE WHEN pv.field_key='medida'           THEN pv.field_value END) as medida,
        MAX(CASE WHEN pv.field_key='modelo_neumatico' THEN pv.field_value END) as modelo,
        MAX(CASE WHEN pv.field_key='tier'             THEN pv.field_value END) as tier,
        MAX(CASE WHEN pv.field_key='codigo_sku'       THEN pv.field_value END) as sku,
        MAX(CASE WHEN pv.field_key='tipo_vehiculo'    THEN pv.field_value END) as tipo_vehiculo
      FROM products p
      LEFT JOIN product_values pv ON pv.product_id = p.id
      WHERE p.active = true
      GROUP BY p.id, p.name, p.brand, p.price_normal, p.stock, p.photo_url
      HAVING MAX(CASE WHEN pv.field_key='medida' THEN pv.field_value END) ILIKE $1
      ORDER BY 
        CASE MAX(CASE WHEN pv.field_key='tier' THEN pv.field_value END)
          WHEN 'Premium' THEN 1 WHEN 'Conveniencia' THEN 2 ELSE 3 END,
        p.brand
      LIMIT 15`,
      [`%${parsed.medida.replace(/\s/g,'')}%`]
    );
    productos = rows;
  }

  res.json({ tire_info: parsed, productos });
}));

// ── COMUNAS CHILE ─────────────────────────────────────────────
app.get('/api/comunas', asyncHandler(async (req, res) => {
  const { search, region } = req.query;
  let q = 'SELECT * FROM comunas_chile WHERE 1=1';
  const params = [];
  let i = 1;
  if (search) { q += ` AND UPPER(comuna) LIKE UPPER($${i})`; params.push('%'+search+'%'); i++; }
  if (region) { q += ` AND region_codigo = $${i}`; params.push(region); i++; }
  q += ' ORDER BY comuna LIMIT 20';
  const { rows } = await query(q, params);
  res.json(rows);
}));

// ── GUARDAR DIRECCIÓN DEL CLIENTE ───────────────────────────
app.patch('/api/leads/:id/address', asyncHandler(async (req, res) => {
  const { address, lat, lng } = req.body;
  await query(
    `UPDATE leads SET 
      address = COALESCE($1, address),
      lat = COALESCE($2, lat),
      lng = COALESCE($3, lng),
      updated_at = NOW()
     WHERE id = $4`,
    [address||null, lat||null, lng||null, req.params.id]
  );
  res.json({ success: true });
}));

// ── DISTANCIA ENTRE PUNTOS (Haversine) ───────────────────────
function haversineKm(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2-lat1)*Math.PI/180;
  const dLon = (lon2-lon1)*Math.PI/180;
  const a = Math.sin(dLat/2)*Math.sin(dLat/2) +
    Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*
    Math.sin(dLon/2)*Math.sin(dLon/2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

app.get('/api/attention/workshops', asyncHandler(async (req, res) => {
  const { fecha, aro, lat, lng } = req.query;

  const { rows: workshops } = await query(`
    SELECT w.*,
      json_agg(DISTINCT jsonb_build_object(
        'dia', ws.dia, 'horas', ws.horas, 'activo', ws.activo
      )) FILTER (WHERE ws.activo=true) as schedules,
      json_agg(DISTINCT jsonb_build_object(
        'tipo', wp.tipo, 'descripcion', wp.descripcion,
        'aro_min', wp.aro_min, 'aro_max', wp.aro_max, 'precio', wp.precio
      )) FILTER (WHERE wp.activo=true) as prices
    FROM workshops w
    LEFT JOIN workshop_schedules ws ON ws.workshop_id=w.id
    LEFT JOIN workshop_prices wp ON wp.workshop_id=w.id
    WHERE w.active=true
    GROUP BY w.id
    ORDER BY w.nombre_comercial
  `);

  const clientLat = parseFloat(lat)||null;
  const clientLng = parseFloat(lng)||null;

  const result = workshops.map(w => {
    let cupos_disponibles = 0;
    let horarios_disponibles = [];

    if (fecha) {
      const diasSemana = ['domingo','lunes','martes','miércoles','jueves','viernes','sábado'];
      const diaSemana = diasSemana[new Date(fecha).getDay()];
      const schedule = (w.schedules||[]).find(s=>s.dia===diaSemana);
      if (schedule?.horas?.length) {
        horarios_disponibles = schedule.horas;
        cupos_disponibles = schedule.horas.length * (w.puestos||1) * (w.turnos_por_puesto||1);
      }
    }

    const aroNum = parseInt(aro)||0;
    const precioMontaje  = (w.prices||[]).find(p => p.tipo==='montaje'  && (!p.aro_min||aroNum>=p.aro_min) && (!p.aro_max||aroNum<=p.aro_max));
    const precioBalanceo = (w.prices||[]).find(p => p.tipo==='balanceo' && (!p.aro_min||aroNum>=p.aro_min) && (!p.aro_max||aroNum<=p.aro_max));

    // Calcular distancia si hay coordenadas del cliente y del taller
    let distancia_km = null;
    if (clientLat && clientLng && w.latitud && w.longitud) {
      distancia_km = Math.round(haversineKm(clientLat, clientLng, parseFloat(w.latitud), parseFloat(w.longitud)) * 10) / 10;
    }

    return {
      ...w,
      cupos_disponibles,
      horarios_disponibles,
      precio_montaje:  precioMontaje?.precio  || null,
      precio_balanceo: precioBalanceo?.precio || null,
      distancia_km,
    };
  }).sort((a,b) => {
    // Ordenar por distancia si está disponible
    if (a.distancia_km !== null && b.distancia_km !== null) return a.distancia_km - b.distancia_km;
    return 0;
  });

  res.json(result);
}));

// ── CONFIGURACIÓN DE ENTREGA ─────────────────────────────────
app.get('/api/delivery-config', asyncHandler(async (req, res) => {
  const { rows } = await query('SELECT * FROM delivery_config ORDER BY key');
  const config = {};
  rows.forEach(r => { config[r.key] = r.value; });
  res.json(config);
}));

app.put('/api/delivery-config', asyncHandler(async (req, res) => {
  for (const [key, value] of Object.entries(req.body)) {
    await query(
      `UPDATE delivery_config SET value=$1, updated_at=NOW() WHERE key=$2`,
      [String(value), key]
    );
  }
  res.json({ success: true });
}));

// ── REGLAS DE NEGOCIO - TIEMPOS DE ENTREGA ───────────────────
app.get('/api/delivery-rules', asyncHandler(async (req, res) => {
  const { tipo } = req.query;
  let q = `SELECT dr.*, 
    CASE WHEN dr.tipo='region' THEN (
      SELECT COUNT(*) FROM delivery_rules dr2 
      WHERE dr2.tipo='comuna' AND dr2.codigo LIKE CONCAT(dr.codigo,'%')
    ) ELSE 0 END as excepciones_count
    FROM delivery_rules dr WHERE 1=1`;
  const params = [];
  if (tipo) { q += ` AND tipo=$1`; params.push(tipo); }
  q += ' ORDER BY tipo DESC, codigo';
  const { rows } = await query(q, params);
  res.json(rows);
}));

app.post('/api/delivery-rules', asyncHandler(async (req, res) => {
  const { tipo, codigo, nombre, horas_entrega, notas } = req.body;
  const { rows: [rule] } = await query(
    `INSERT INTO delivery_rules (id, tipo, codigo, nombre, horas_entrega, notas)
     VALUES ($1,$2,$3,$4,$5,$6)
     ON CONFLICT (tipo, codigo) DO UPDATE SET
       horas_entrega=$5, notas=$6, activo=true, updated_at=NOW()
     RETURNING *`,
    [require('crypto').randomUUID(), tipo, codigo, nombre, 
     horas_entrega === null ? null : parseInt(horas_entrega)||null, notas||null]
  );
  res.status(201).json(rule);
}));

app.put('/api/delivery-rules/:id', asyncHandler(async (req, res) => {
  const { horas_entrega, notas, activo } = req.body;
  const { rows: [rule] } = await query(
    `UPDATE delivery_rules SET horas_entrega=$1, notas=$2, activo=$3, updated_at=NOW()
     WHERE id=$4 RETURNING *`,
    [horas_entrega===null?null:parseInt(horas_entrega)||null, notas||null, activo!==false, req.params.id]
  );
  res.json(rule);
}));

app.delete('/api/delivery-rules/:id', asyncHandler(async (req, res) => {
  await query('DELETE FROM delivery_rules WHERE id=$1', [req.params.id]);
  res.json({ success: true });
}));

// Consultar tiempo de entrega para una comuna específica
app.get('/api/delivery-rules/lookup', asyncHandler(async (req, res) => {
  const { comuna_codigo } = req.query;
  if (!comuna_codigo) return res.status(400).json({ error: 'comuna_codigo requerido' });

  // Primero buscar excepción por comuna
  const { rows: [comunaRule] } = await query(
    `SELECT * FROM delivery_rules WHERE tipo='comuna' AND codigo=$1 AND activo=true`,
    [comuna_codigo]
  );
  if (comunaRule) return res.json({ ...comunaRule, source: 'comuna' });

  // Si no, buscar regla por región
  const region_codigo = comuna_codigo.slice(0,2);
  const { rows: [regionRule] } = await query(
    `SELECT * FROM delivery_rules WHERE tipo='region' AND codigo=$1 AND activo=true`,
    [region_codigo]
  );
  if (regionRule) return res.json({ ...regionRule, source: 'region' });

  res.json({ horas_entrega: null, source: 'no_encontrado' });
}));
