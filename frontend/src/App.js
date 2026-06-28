import React, { useState, useEffect, useCallback } from 'react';
import {
  LineChart, Line, BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from 'recharts';

const API = 'https://real-time-ecommerce-data-pipeline-production.up.railway.app/api/dashboard';

const PIE_COLORS  = ['#7c3aed','#06b6d4','#10b981','#f59e0b','#f43f5e','#6366f1','#ec4899'];
const CITY_COLORS = ['#7c3aed','#6366f1','#06b6d4','#0ea5e9','#10b981','#34d399','#f59e0b','#fbbf24','#f43f5e','#fb7185'];

const inr   = n => '₹' + Number(n).toLocaleString('en-IN', { maximumFractionDigits:0 });
const inrCr = n => {
  if (n >= 1e7) return '₹' + (n/1e7).toFixed(2) + ' Cr';
  if (n >= 1e5) return '₹' + (n/1e5).toFixed(1) + 'L';
  return inr(n);
};

const ttStyle = {
  background:'#0c1220', border:'1px solid rgba(124,58,237,0.4)',
  borderRadius:10, color:'#e2e8f0', fontSize:12,
  boxShadow:'0 8px 32px rgba(0,0,0,0.6)',
};

// Helper: get date string YYYY-MM-DD from N days ago
const daysAgo = (n) => {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().split('T')[0];
};

const today = () => new Date().toISOString().split('T')[0];

const globalCSS = `
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Space+Grotesk:wght@500;700&display=swap');
*,*::before,*::after{margin:0;padding:0;box-sizing:border-box}
body{background:#04060f;color:#e2e8f0;font-family:'Inter',sans-serif;min-height:100vh;overflow-x:hidden}
body::before{content:'';position:fixed;top:-40%;left:-20%;width:600px;height:600px;background:radial-gradient(circle,rgba(124,58,237,0.08) 0%,transparent 70%);pointer-events:none;z-index:0}
body::after{content:'';position:fixed;bottom:-30%;right:-10%;width:500px;height:500px;background:radial-gradient(circle,rgba(6,182,212,0.06) 0%,transparent 70%);pointer-events:none;z-index:0}
::-webkit-scrollbar{width:6px}
::-webkit-scrollbar-track{background:#04060f}
::-webkit-scrollbar-thumb{background:rgba(124,58,237,0.4);border-radius:3px}
@keyframes pulse-dot{0%,100%{opacity:1;box-shadow:0 0 0 0 rgba(16,185,129,0.6)}50%{opacity:0.7;box-shadow:0 0 0 6px rgba(16,185,129,0)}}
@keyframes fadeUp{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:translateY(0)}}
@keyframes spin{to{transform:rotate(360deg)}}
input[type="date"]::-webkit-calendar-picker-indicator{filter:invert(0.5) sepia(1) saturate(2) hue-rotate(220deg);cursor:pointer;opacity:0.7}
input[type="date"]::-webkit-calendar-picker-indicator:hover{opacity:1}
`;

function LiveBadge({ lastUpdated, refreshIn }) {
  return (
    <div style={{display:'flex',alignItems:'center',gap:16}}>
      <div style={{display:'flex',alignItems:'center',gap:8,background:'rgba(16,185,129,0.08)',border:'1px solid rgba(16,185,129,0.25)',borderRadius:20,padding:'6px 14px',fontSize:12,fontWeight:600,color:'#10b981'}}>
        <span style={{width:8,height:8,background:'#10b981',borderRadius:'50%',animation:'pulse-dot 1.5s infinite',display:'inline-block'}}/>
        LIVE
      </div>
      <div style={{textAlign:'right'}}>
        <div style={{color:'#64748b',fontSize:11}}>Refreshes in <span style={{color:'#06b6d4',fontWeight:600}}>{refreshIn}s</span></div>
        {lastUpdated && <div style={{color:'#334155',fontSize:10,marginTop:2}}>Updated {lastUpdated}</div>}
      </div>
    </div>
  );
}

function KPICard({ icon, label, value, sub, color, delay }) {
  const gradients = {
    violet:'linear-gradient(135deg,#7c3aed,#6366f1)',
    cyan:'linear-gradient(135deg,#06b6d4,#0ea5e9)',
    emerald:'linear-gradient(135deg,#10b981,#34d399)',
    amber:'linear-gradient(135deg,#f59e0b,#fbbf24)',
  };
  const glows = {
    violet:'rgba(124,58,237,0.15)',cyan:'rgba(6,182,212,0.15)',
    emerald:'rgba(16,185,129,0.15)',amber:'rgba(245,158,11,0.15)',
  };
  return (
    <div style={{background:'#0c1220',borderRadius:18,padding:'22px 24px',border:'1px solid rgba(255,255,255,0.06)',position:'relative',overflow:'hidden',animation:`fadeUp 0.5s ease ${delay}s both`,transition:'transform 0.2s,border-color 0.2s,box-shadow 0.2s'}}
      onMouseEnter={e=>{e.currentTarget.style.transform='translateY(-4px)';e.currentTarget.style.borderColor='rgba(255,255,255,0.12)';e.currentTarget.style.boxShadow=`0 20px 60px ${glows[color]}`}}
      onMouseLeave={e=>{e.currentTarget.style.transform='translateY(0)';e.currentTarget.style.borderColor='rgba(255,255,255,0.06)';e.currentTarget.style.boxShadow='none'}}>
      <div style={{position:'absolute',top:0,left:0,right:0,height:2,background:gradients[color]}}/>
      <div style={{position:'absolute',top:-20,right:-20,width:80,height:80,background:glows[color],borderRadius:'50%',filter:'blur(20px)'}}/>
      <div style={{fontSize:26,marginBottom:12}}>{icon}</div>
      <div style={{color:'#64748b',fontSize:11,fontWeight:600,letterSpacing:'0.8px',textTransform:'uppercase',marginBottom:6}}>{label}</div>
      <div style={{fontSize:28,fontWeight:800,lineHeight:1,fontFamily:'Space Grotesk,sans-serif',background:gradients[color],WebkitBackgroundClip:'text',WebkitTextFillColor:'transparent'}}>{value}</div>
      {sub && <div style={{color:'#10b981',fontSize:11,marginTop:8,fontWeight:500}}>{sub}</div>}
    </div>
  );
}

function Card({ title, badge, children, style={} }) {
  return (
    <div style={{background:'#0c1220',borderRadius:18,padding:'22px 24px',border:'1px solid rgba(255,255,255,0.06)',transition:'border-color 0.2s',...style}}
      onMouseEnter={e=>e.currentTarget.style.borderColor='rgba(255,255,255,0.12)'}
      onMouseLeave={e=>e.currentTarget.style.borderColor='rgba(255,255,255,0.06)'}>
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:20}}>
        <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:'1px'}}>{title}</div>
        {badge && <div style={{background:'rgba(124,58,237,0.12)',color:'#a78bfa',fontSize:11,padding:'3px 10px',borderRadius:20,fontWeight:600,border:'1px solid rgba(124,58,237,0.2)'}}>{badge}</div>}
      </div>
      {children}
    </div>
  );
}

// ── DATE PRESETS ──────────────────────────────────────────────────────────────
const PRESETS = [
  { label: 'Last 7d',  days: 7  },
  { label: 'Last 30d', days: 30 },
  { label: 'Last 90d', days: 90 },
  { label: 'Custom',   days: null },
];

function DateFilterBar({ datePreset, setDatePreset, customRange, setCustomRange }) {
  const isCustom = datePreset === 'Custom';
  const hasCustom = isCustom && customRange.from && customRange.to;

  const activePreset = PRESETS.find(p => p.label === datePreset);
  const isFiltered   = datePreset !== null;

  return (
    <div style={{
      display:'flex', gap:10, alignItems:'center', flexWrap:'wrap',
      marginBottom:10,
      background:'#0c1220',
      border:`1px solid ${isFiltered ? 'rgba(99,102,241,0.35)' : 'rgba(255,255,255,0.06)'}`,
      borderRadius:14, padding:'14px 18px',
      transition:'border-color 0.2s',
    }}>
      {/* Label */}
      <span style={{color:'#64748b',fontSize:12,fontWeight:600,marginRight:4,letterSpacing:'0.5px'}}>📅 DATE</span>

      {/* Preset buttons */}
      <div style={{display:'flex',gap:6}}>
        {PRESETS.map(p => {
          const active = datePreset === p.label;
          return (
            <button
              key={p.label}
              onClick={() => {
                setDatePreset(p.label);
                if (p.label !== 'Custom') setCustomRange({ from: '', to: '' });
              }}
              style={{
                background: active
                  ? 'linear-gradient(135deg,rgba(99,102,241,0.25),rgba(6,182,212,0.15))'
                  : 'rgba(15,23,42,0.7)',
                border: `1px solid ${active ? 'rgba(99,102,241,0.6)' : 'rgba(255,255,255,0.08)'}`,
                borderRadius: 9,
                color: active ? '#a78bfa' : '#94a3b8',
                padding: '7px 14px',
                fontSize: 12,
                fontWeight: active ? 700 : 500,
                cursor: 'pointer',
                fontFamily: 'Inter,sans-serif',
                transition: 'all 0.15s',
                outline: 'none',
              }}
              onMouseEnter={e => { if (!active) { e.currentTarget.style.borderColor='rgba(99,102,241,0.35)'; e.currentTarget.style.color='#c4b5fd'; }}}
              onMouseLeave={e => { if (!active) { e.currentTarget.style.borderColor='rgba(255,255,255,0.08)'; e.currentTarget.style.color='#94a3b8'; }}}
            >
              {p.label}
            </button>
          );
        })}
      </div>

      {/* Custom date inputs — show only when Custom is selected */}
      {isCustom && (
        <div style={{display:'flex',gap:8,alignItems:'center'}}>
          <span style={{color:'#475569',fontSize:11}}>From</span>
          <input
            type="date"
            value={customRange.from}
            max={customRange.to || today()}
            onChange={e => setCustomRange(r => ({ ...r, from: e.target.value }))}
            style={{
              background:'rgba(15,23,42,0.9)',
              border:`1px solid ${customRange.from ? 'rgba(99,102,241,0.5)' : 'rgba(255,255,255,0.08)'}`,
              borderRadius:9, color:'#e2e8f0', padding:'6px 10px',
              fontSize:12, fontFamily:'Inter,sans-serif', cursor:'pointer', outline:'none',
              colorScheme:'dark',
            }}
          />
          <span style={{color:'#475569',fontSize:11}}>To</span>
          <input
            type="date"
            value={customRange.to}
            min={customRange.from}
            max={today()}
            onChange={e => setCustomRange(r => ({ ...r, to: e.target.value }))}
            style={{
              background:'rgba(15,23,42,0.9)',
              border:`1px solid ${customRange.to ? 'rgba(99,102,241,0.5)' : 'rgba(255,255,255,0.08)'}`,
              borderRadius:9, color:'#e2e8f0', padding:'6px 10px',
              fontSize:12, fontFamily:'Inter,sans-serif', cursor:'pointer', outline:'none',
              colorScheme:'dark',
            }}
          />
        </div>
      )}

      {/* Reset */}
      {isFiltered && (
        <button
          onClick={() => { setDatePreset(null); setCustomRange({ from: '', to: '' }); }}
          style={{
            background:'rgba(244,63,94,0.08)',
            border:'1px solid rgba(244,63,94,0.25)',
            borderRadius:9, color:'#f43f5e',
            padding:'7px 13px', fontSize:12, cursor:'pointer',
            fontWeight:600, fontFamily:'Inter,sans-serif',
          }}
        >
          ✕ Clear
        </button>
      )}

      {/* Active label on right */}
      <div style={{marginLeft:'auto',color:'#64748b',fontSize:11}}>
        {!isFiltered && 'All time'}
        {isFiltered && !isCustom && (
          <span style={{color:'#a78bfa'}}>
            {daysAgo(activePreset.days)} → {today()}
          </span>
        )}
        {isCustom && hasCustom && (
          <span style={{color:'#a78bfa'}}>{customRange.from} → {customRange.to}</span>
        )}
        {isCustom && !hasCustom && (
          <span style={{color:'#475569'}}>Pick a range above</span>
        )}
      </div>
    </div>
  );
}

function FilterBar({ cities, categories, filters, setFilters }) {
  const active = filters.city !== 'all' || filters.category !== 'all';
  return (
    <div style={{display:'flex',gap:10,alignItems:'center',flexWrap:'wrap',marginBottom:24,background:'#0c1220',border:`1px solid ${active?'rgba(124,58,237,0.3)':'rgba(255,255,255,0.06)'}`,borderRadius:14,padding:'14px 18px'}}>
      <span style={{color:'#64748b',fontSize:12,fontWeight:600,marginRight:4}}>🔍 FILTER</span>
      {[
        {key:'city',opts:['all',...cities],display:v=>v==='all'?'All Cities':v},
        {key:'category',opts:['all',...categories],display:v=>v==='all'?'All Categories':v}
      ].map(({key,opts,display})=>(
        <select key={key} value={filters[key]} onChange={e=>setFilters(f=>({...f,[key]:e.target.value}))}
          style={{background:'rgba(15,23,42,0.9)',border:`1px solid ${filters[key]!=='all'?'rgba(124,58,237,0.5)':'rgba(255,255,255,0.08)'}`,borderRadius:9,color:filters[key]!=='all'?'#a78bfa':'#e2e8f0',padding:'7px 14px',fontSize:13,cursor:'pointer',outline:'none',fontFamily:'Inter,sans-serif'}}>
          {opts.map(o=><option key={o} value={o}>{display(o)}</option>)}
        </select>
      ))}
      {active && (
        <button onClick={()=>setFilters({city:'all',category:'all'})}
          style={{background:'rgba(244,63,94,0.1)',border:'1px solid rgba(244,63,94,0.3)',borderRadius:9,color:'#f43f5e',padding:'7px 14px',fontSize:12,cursor:'pointer',fontWeight:600}}>
          ✕ Reset
        </button>
      )}
      <div style={{marginLeft:'auto',color:'#64748b',fontSize:11}}>
        {active
          ? <span style={{color:'#a78bfa'}}>
              {filters.city!=='all'?`City: ${filters.city}`:''} 
              {filters.city!=='all'&&filters.category!=='all'?' · ':''}
              {filters.category!=='all'?`Category: ${filters.category}`:''}
            </span>
          : 'Showing all data'
        }
      </div>
    </div>
  );
}

export default function App() {
  const [data,setData]               = useState({});
  const [loading,setLoading]         = useState(true);
  const [filters,setFilters]         = useState({city:'all',category:'all'});

  // ── Date filter state ──
  const [datePreset,setDatePreset]       = useState(null);           // null | '7d' | '30d' | '90d' | 'Custom'
  const [customRange,setCustomRange]     = useState({from:'',to:''}); // used only when datePreset==='Custom'

  const [lastUpdated,setLastUpdated] = useState(null);
  const [refreshIn,setRefreshIn]     = useState(30);
  const [allCities,setAllCities]     = useState([]);
  const [allCategories,setAllCategories] = useState([]);

  // Compute effective date_from / date_to from current preset/custom state
  const getDateParams = useCallback(() => {
    if (!datePreset) return { date_from: null, date_to: null };
    if (datePreset === 'Custom') {
      return {
        date_from: customRange.from || null,
        date_to:   customRange.to   || null,
      };
    }
    const preset = PRESETS.find(p => p.label === datePreset);
    if (!preset || !preset.days) return { date_from: null, date_to: null };
    return { date_from: daysAgo(preset.days), date_to: today() };
  }, [datePreset, customRange]);

  const fetchAll = useCallback(async () => {
    const params = new URLSearchParams();
    if (filters.city !== 'all') params.append('city', filters.city);
    if (filters.category !== 'all') params.append('category', filters.category);

    const { date_from, date_to } = getDateParams();
    if (date_from) params.append('date_from', date_from);
    if (date_to)   params.append('date_to',   date_to);

    // Don't fetch if Custom selected but range incomplete
    if (datePreset === 'Custom' && (!customRange.from || !customRange.to)) return;

    const q = params.toString() ? `?${params.toString()}` : '';

    try {
      const [summary,city,cat,status,monthly,products,prediction,catForecast,inventory,churn] = await Promise.all([
        fetch(`${API}/summary${q}`).then(r=>r.json()),
        fetch(`${API}/revenue-by-city${q}`).then(r=>r.json()),
        fetch(`${API}/category-sales${q}`).then(r=>r.json()),
        fetch(`${API}/order-status${q}`).then(r=>r.json()),
        fetch(`${API}/monthly-revenue${q}`).then(r=>r.json()),
        fetch(`${API}/top-products${q}`).then(r=>r.json()),
        fetch(`${API}/sales-prediction${q}`).then(r=>r.json()),
        fetch(`${API}/category-forecast${q}`).then(r=>r.json()),
        fetch(`${API}/inventory-alert${q}`).then(r=>r.json()),
        fetch(`${API}/customer-churn${q}`).then(r=>r.json()),
      ]);
      setData({summary,city,cat,status,monthly,products,prediction,catForecast,inventory,churn});
      setLastUpdated(new Date().toLocaleTimeString());
      setRefreshIn(30);

      // Sirf pehli baar cities/categories set karo (no filter applied)
      if (!params.toString()) {
        setAllCities((city||[]).map(d=>d.city));
        setAllCategories((cat||[]).map(d=>d.category));
      }
    } catch(e) { console.error('Fetch error:',e); }
    finally { setLoading(false); }
  }, [filters, getDateParams, datePreset, customRange]);

  useEffect(()=>{ fetchAll(); },[fetchAll]);
  useEffect(()=>{ const i=setInterval(fetchAll,30000); return()=>clearInterval(i); },[fetchAll]);
  useEffect(()=>{ const t=setInterval(()=>setRefreshIn(r=>r>0?r-1:30),1000); return()=>clearInterval(t); },[]);

  if (loading) return (
    <div style={{display:'flex',alignItems:'center',justifyContent:'center',height:'100vh',flexDirection:'column',gap:20,background:'#04060f'}}>
      <style>{globalCSS}</style>
      <div style={{width:48,height:48,border:'3px solid rgba(124,58,237,0.2)',borderTopColor:'#7c3aed',borderRadius:'50%',animation:'spin 0.8s linear infinite'}}/>
      <div style={{fontFamily:'Space Grotesk,sans-serif',fontSize:14,color:'#64748b',letterSpacing:2}}>LOADING ANALYTICS...</div>
    </div>
  );

  const s = data.summary || {};
  const cityData = data.city || [];
  const catData  = data.cat  || [];

  return (
    <>
      <style>{globalCSS}</style>
      <header style={{position:'sticky',top:0,zIndex:100,background:'rgba(4,6,15,0.85)',backdropFilter:'blur(24px)',borderBottom:'1px solid rgba(124,58,237,0.15)',padding:'16px 32px',display:'flex',alignItems:'center',justifyContent:'space-between'}}>
        <div>
          <div style={{fontFamily:'Space Grotesk,sans-serif',fontSize:20,fontWeight:700,background:'linear-gradient(135deg,#a78bfa 0%,#06b6d4 50%,#10b981 100%)',WebkitBackgroundClip:'text',WebkitTextFillColor:'transparent'}}>
            ⚡ E-Commerce Analytics
          </div>
          <div style={{color:'#334155',fontSize:11,marginTop:2}}>Kafka → PostgreSQL → Apache Airflow → FastAPI → React</div>
        </div>
        <LiveBadge lastUpdated={lastUpdated} refreshIn={refreshIn}/>
      </header>

      <main style={{padding:'28px 32px',position:'relative',zIndex:1}}>

        {/* DATE FILTER — upar */}
        <DateFilterBar
          datePreset={datePreset}
          setDatePreset={setDatePreset}
          customRange={customRange}
          setCustomRange={setCustomRange}
        />

        {/* CITY + CATEGORY FILTER */}
        <FilterBar cities={allCities} categories={allCategories} filters={filters} setFilters={setFilters}/>

        {/* KPI CARDS */}
        <div style={{display:'grid',gridTemplateColumns:'repeat(4,1fr)',gap:20,marginBottom:24}}>
          <KPICard icon="📦" label="Total Orders"     value={s.total_orders?.toLocaleString()}    color="violet"  delay={0}    sub="All time"/>
          <KPICard icon="💰" label="Total Revenue"    value={inrCr(s.total_revenue)}              color="cyan"    delay={0.08} sub="Gross revenue"/>
          <KPICard icon="👥" label="Unique Customers" value={s.total_customers?.toLocaleString()} color="emerald" delay={0.16} sub="Distinct buyers"/>
          <KPICard icon="🎯" label="Avg Order Value"  value={inrCr(s.average_order_value)}        color="amber"   delay={0.24} sub="Per transaction"/>
        </div>

        {/* ROW 1 */}
        <div style={{display:'grid',gridTemplateColumns:'2fr 1fr',gap:20,marginBottom:20}}>
          <Card title="Monthly Revenue Trend" badge={`${(data.monthly||[]).length} months`}>
            <ResponsiveContainer width="100%" height={260}>
              <LineChart data={data.monthly||[]}>
                <defs>
                  <linearGradient id="lineGrad" x1="0" y1="0" x2="1" y2="0">
                    <stop offset="0%" stopColor="#7c3aed"/><stop offset="100%" stopColor="#06b6d4"/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)"/>
                <XAxis dataKey="month" stroke="#64748b" tick={{fontSize:11,fill:'#64748b'}}/>
                <YAxis stroke="#64748b" tick={{fontSize:11,fill:'#64748b'}} tickFormatter={v=>`₹${(v/1e5).toFixed(0)}L`}/>
                <Tooltip contentStyle={ttStyle} formatter={v=>[inr(v),'Revenue']}/>
                <Line type="monotone" dataKey="revenue" stroke="url(#lineGrad)" strokeWidth={3} dot={{fill:'#7c3aed',r:4,strokeWidth:0}} activeDot={{r:6,fill:'#a78bfa',strokeWidth:0}}/>
              </LineChart>
            </ResponsiveContainer>
          </Card>

          <Card title="Orders by Status">
            <ResponsiveContainer width="100%" height={260}>
              <PieChart>
                <Pie data={data.status||[]} dataKey="count" nameKey="status" cx="50%" cy="45%" outerRadius={90} innerRadius={50} paddingAngle={3}>
                  {(data.status||[]).map((_,i)=><Cell key={i} fill={PIE_COLORS[i%PIE_COLORS.length]} stroke="transparent"/>)}
                </Pie>
                <Tooltip contentStyle={ttStyle} formatter={(v,n)=>[v.toLocaleString(),n]}/>
                <Legend iconType="circle" iconSize={8} formatter={v=><span style={{color:'#64748b',fontSize:11}}>{v}</span>}/>
              </PieChart>
            </ResponsiveContainer>
          </Card>
        </div>

        {/* ROW 2 */}
        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:20,marginBottom:20}}>
          <Card title="Revenue by City" badge={`Top ${Math.min(cityData.length,10)}`}>
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={cityData.slice(0,10)} barSize={22}>
                <defs>
                  {CITY_COLORS.map((c,i)=>(
                    <linearGradient key={i} id={`cg${i}`} x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={c} stopOpacity={0.9}/><stop offset="100%" stopColor={c} stopOpacity={0.4}/>
                    </linearGradient>
                  ))}
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false}/>
                <XAxis dataKey="city" stroke="#64748b" tick={{fontSize:10,fill:'#64748b'}}/>
                <YAxis stroke="#64748b" tick={{fontSize:11,fill:'#64748b'}} tickFormatter={v=>`₹${(v/1e5).toFixed(0)}L`}/>
                <Tooltip contentStyle={ttStyle} formatter={v=>[inr(v),'Revenue']} cursor={{fill:'rgba(255,255,255,0.03)'}}/>
                <Bar dataKey="revenue" radius={[6,6,0,0]}>
                  {cityData.slice(0,10).map((_,i)=><Cell key={i} fill={`url(#cg${i%CITY_COLORS.length})`}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </Card>

          <Card title="Revenue by Category" badge={`${catData.length} categories`}>
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie data={catData} dataKey="sales" nameKey="category" cx="50%" cy="45%" outerRadius={100} innerRadius={55} paddingAngle={3}>
                  {catData.map((_,i)=><Cell key={i} fill={PIE_COLORS[i%PIE_COLORS.length]} stroke="transparent"/>)}
                </Pie>
                <Tooltip contentStyle={ttStyle} formatter={v=>[inr(v),'Sales']}/>
                <Legend iconType="circle" iconSize={8} formatter={v=><span style={{color:'#64748b',fontSize:11}}>{v}</span>}/>
              </PieChart>
            </ResponsiveContainer>
          </Card>
        </div>

        {/* TOP PRODUCTS */}
        <Card title="Top 10 Products" badge="By Revenue" style={{marginBottom:20}}>
          <div style={{overflowX:'auto'}}>
            <table style={{width:'100%',borderCollapse:'collapse'}}>
              <thead>
                <tr style={{borderBottom:'1px solid #1e2d40'}}>
                  {['#','Product','Qty Sold','Revenue','Share'].map(h=>(
                    <th key={h} style={{padding:'10px 14px',textAlign:'left',color:'#475569',fontSize:11,fontWeight:700,textTransform:'uppercase',letterSpacing:'0.6px'}}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(data.products||[]).slice(0,10).map((row,i)=>{
                  const maxRev = (data.products||[])[0]?.total_revenue||1;
                  const pct = ((row.total_revenue/maxRev)*100).toFixed(0);
                  return (
                    <tr key={i} style={{borderBottom:'1px solid rgba(255,255,255,0.03)',transition:'background 0.15s'}}
                      onMouseEnter={e=>e.currentTarget.style.background='rgba(124,58,237,0.05)'}
                      onMouseLeave={e=>e.currentTarget.style.background='transparent'}>
                      <td style={{padding:'13px 14px',color:'#64748b',fontWeight:700,width:40}}>{i===0?'🥇':i===1?'🥈':i===2?'🥉':`#${i+1}`}</td>
                      <td style={{padding:'13px 14px',color:'#e2e8f0',fontSize:13,fontWeight:500}}>{row.product_name}</td>
                      <td style={{padding:'13px 14px',color:'#64748b',fontSize:13}}>{row.total_quantity?.toLocaleString()}</td>
                      <td style={{padding:'13px 14px',color:'#10b981',fontWeight:700,fontSize:13}}>{inr(row.total_revenue)}</td>
                      <td style={{padding:'13px 14px',minWidth:120}}>
                        <div style={{background:'rgba(255,255,255,0.05)',borderRadius:4,height:6,overflow:'hidden'}}>
                          <div style={{height:'100%',borderRadius:4,width:`${pct}%`,background:'linear-gradient(90deg,#7c3aed,#06b6d4)',transition:'width 0.6s ease'}}/>
                        </div>
                        <div style={{color:'#64748b',fontSize:10,marginTop:3}}>{pct}%</div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Card>

        {/* SALES PREDICTION */}
        <Card title="📈 Sales Forecast — Next 7 Days" badge="AI Prediction" style={{marginBottom:20}}>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={[
              ...(data.prediction?.actual||[]).map(d=>({day:d.day,actual:d.revenue,predicted:null})),
              ...(data.prediction?.predicted||[]).map(d=>({day:d.day,actual:null,predicted:d.predicted})),
            ]}>
              <defs>
                <linearGradient id="actualGrad" x1="0" y1="0" x2="1" y2="0">
                  <stop offset="0%" stopColor="#06b6d4"/><stop offset="100%" stopColor="#6366f1"/>
                </linearGradient>
                <linearGradient id="predGrad" x1="0" y1="0" x2="1" y2="0">
                  <stop offset="0%" stopColor="#f59e0b"/><stop offset="100%" stopColor="#f43f5e"/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)"/>
              <XAxis dataKey="day" stroke="#64748b" tick={{fontSize:10,fill:'#64748b'}}/>
              <YAxis stroke="#64748b" tick={{fontSize:11,fill:'#64748b'}} tickFormatter={v=>`₹${(v/1e5).toFixed(0)}L`}/>
              <Tooltip contentStyle={ttStyle} formatter={(v,n)=>[inr(v),n==='actual'?'Actual Revenue':'Predicted Revenue']}/>
              <Legend formatter={v=><span style={{color:'#94a3b8',fontSize:11}}>{v==='actual'?'Actual Revenue':'Predicted Revenue'}</span>}/>
              <Line type="monotone" dataKey="actual" stroke="url(#actualGrad)" strokeWidth={3} dot={{r:3,fill:'#06b6d4',strokeWidth:0}} connectNulls={false} name="actual"/>
              <Line type="monotone" dataKey="predicted" stroke="url(#predGrad)" strokeWidth={2} strokeDasharray="6 3" dot={{r:4,fill:'#f59e0b',strokeWidth:0}} connectNulls={false} name="predicted"/>
            </LineChart>
          </ResponsiveContainer>
          <div style={{display:'flex',gap:16,marginTop:12,flexWrap:'wrap'}}>
            {(data.prediction?.predicted||[]).slice(0,3).map((d,i)=>(
              <div key={i} style={{background:'rgba(245,158,11,0.08)',border:'1px solid rgba(245,158,11,0.2)',borderRadius:10,padding:'10px 16px',flex:1}}>
                <div style={{color:'#64748b',fontSize:10,marginBottom:4}}>{d.day}</div>
                <div style={{color:'#f59e0b',fontWeight:700,fontSize:15}}>{inr(d.predicted)}</div>
              </div>
            ))}
          </div>
        </Card>

        {/* CATEGORY FORECAST + CHURN */}
        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:20,marginBottom:20}}>
          <Card title="🔮 Category Demand Forecast" badge="Last 90 days">
            <ResponsiveContainer width="100%" height={260}>
              <BarChart data={(data.catForecast||[]).slice(0,6)} barSize={28}>
                <defs>
                  <linearGradient id="growGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#10b981" stopOpacity={0.9}/><stop offset="100%" stopColor="#10b981" stopOpacity={0.3}/>
                  </linearGradient>
                  <linearGradient id="fallGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#f43f5e" stopOpacity={0.9}/><stop offset="100%" stopColor="#f43f5e" stopOpacity={0.3}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false}/>
                <XAxis dataKey="category" stroke="#64748b" tick={{fontSize:10,fill:'#64748b'}}/>
                <YAxis stroke="#64748b" tick={{fontSize:10,fill:'#64748b'}} tickFormatter={v=>`${v>0?'+':''}${v.toFixed(0)}%`}/>
                <Tooltip contentStyle={ttStyle} formatter={v=>[`${v>0?'+':''}${v}%`,'Growth']}/>
                <Bar dataKey="growth_pct" radius={[6,6,0,0]}>
                  {(data.catForecast||[]).slice(0,6).map((d,i)=>(
                    <Cell key={i} fill={d.growth_pct>=0?'url(#growGrad)':'url(#fallGrad)'}/>
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            <div style={{display:'flex',gap:8,flexWrap:'wrap',marginTop:12}}>
              {(data.catForecast||[]).slice(0,6).map((d,i)=>(
                <div key={i} style={{background:d.trend==='rising'?'rgba(16,185,129,0.08)':d.trend==='falling'?'rgba(244,63,94,0.08)':'rgba(100,116,139,0.08)',border:`1px solid ${d.trend==='rising'?'rgba(16,185,129,0.2)':d.trend==='falling'?'rgba(244,63,94,0.2)':'rgba(100,116,139,0.2)'}`,borderRadius:8,padding:'6px 12px',fontSize:11,color:d.trend==='rising'?'#10b981':d.trend==='falling'?'#f43f5e':'#64748b',fontWeight:600}}>
                  {d.trend==='rising'?'↑':d.trend==='falling'?'↓':'→'} {d.category}
                </div>
              ))}
            </div>
          </Card>

          <Card title="👤 Customer Churn Analysis" badge="Risk Segmentation">
            {data.churn && (
              <>
                <div style={{display:'grid',gridTemplateColumns:'repeat(3,1fr)',gap:12,marginBottom:20}}>
                  {[
                    {label:'Active',value:data.churn.summary?.active,color:'#10b981',bg:'rgba(16,185,129,0.08)',border:'rgba(16,185,129,0.2)',icon:'✅'},
                    {label:'At Risk',value:data.churn.summary?.at_risk,color:'#f59e0b',bg:'rgba(245,158,11,0.08)',border:'rgba(245,158,11,0.2)',icon:'⚠️'},
                    {label:'Churned',value:data.churn.summary?.churned,color:'#f43f5e',bg:'rgba(244,63,94,0.08)',border:'rgba(244,63,94,0.2)',icon:'❌'},
                  ].map(c=>(
                    <div key={c.label} style={{background:c.bg,border:`1px solid ${c.border}`,borderRadius:12,padding:'14px',textAlign:'center'}}>
                      <div style={{fontSize:20,marginBottom:6}}>{c.icon}</div>
                      <div style={{color:c.color,fontSize:22,fontWeight:800,fontFamily:'Space Grotesk,sans-serif'}}>{c.value?.toLocaleString()}</div>
                      <div style={{color:'#64748b',fontSize:11,marginTop:4}}>{c.label}</div>
                    </div>
                  ))}
                </div>
                <div style={{fontSize:11,color:'#64748b',marginBottom:10,fontWeight:600,textTransform:'uppercase',letterSpacing:'0.6px'}}>High-Value Churned</div>
                <div style={{maxHeight:160,overflowY:'auto'}}>
                  {(data.churn.high_value_at_risk||[]).slice(0,5).map((c,i)=>(
                    <div key={i} style={{display:'flex',justifyContent:'space-between',alignItems:'center',padding:'8px 0',borderBottom:'1px solid rgba(255,255,255,0.04)'}}>
                      <div>
                        <div style={{color:'#e2e8f0',fontSize:12,fontWeight:500}}>{c.customer_name}</div>
                        <div style={{color:'#64748b',fontSize:10}}>{c.days_since_last} days inactive · {c.total_orders} orders</div>
                      </div>
                      <div style={{color:'#f43f5e',fontWeight:700,fontSize:12}}>{inr(c.total_spent)}</div>
                    </div>
                  ))}
                </div>
              </>
            )}
          </Card>
        </div>

        {/* INVENTORY ALERT */}
        <Card title="⚠️ Inventory Demand Alert" badge="Last 90 days" style={{marginBottom:20}}>
          <div style={{overflowX:'auto'}}>
            <table style={{width:'100%',borderCollapse:'collapse'}}>
              <thead>
                <tr style={{borderBottom:'1px solid #1e2d40'}}>
                  {['Product','Category','Units Sold','Orders/Day','Last Ordered','Risk'].map(h=>(
                    <th key={h} style={{padding:'10px 14px',textAlign:'left',color:'#475569',fontSize:11,fontWeight:700,textTransform:'uppercase',letterSpacing:'0.6px'}}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(data.inventory||[]).map((row,i)=>{
                  const rc={
                    critical:{color:'#f43f5e',bg:'rgba(244,63,94,0.1)',border:'rgba(244,63,94,0.3)',label:'🔴 Critical'},
                    high:    {color:'#f59e0b',bg:'rgba(245,158,11,0.1)',border:'rgba(245,158,11,0.3)',label:'🟠 High'},
                    medium:  {color:'#06b6d4',bg:'rgba(6,182,212,0.1)', border:'rgba(6,182,212,0.3)', label:'🔵 Medium'},
                    low:     {color:'#10b981',bg:'rgba(16,185,129,0.1)',border:'rgba(16,185,129,0.3)',label:'🟢 Low'},
                  }[row.risk_level]||{color:'#10b981',bg:'rgba(16,185,129,0.1)',border:'rgba(16,185,129,0.3)',label:'🟢 Low'};
                  return (
                    <tr key={i} style={{borderBottom:'1px solid rgba(255,255,255,0.03)',transition:'background 0.15s'}}
                      onMouseEnter={e=>e.currentTarget.style.background='rgba(124,58,237,0.05)'}
                      onMouseLeave={e=>e.currentTarget.style.background='transparent'}>
                      <td style={{padding:'12px 14px',color:'#e2e8f0',fontSize:13,fontWeight:500}}>{row.product_name}</td>
                      <td style={{padding:'12px 14px',color:'#64748b',fontSize:12}}>{row.category}</td>
                      <td style={{padding:'12px 14px',color:'#a78bfa',fontWeight:600,fontSize:13}}>{row.total_sold?.toLocaleString()}</td>
                      <td style={{padding:'12px 14px',color:'#06b6d4',fontSize:13,fontWeight:600}}>{row.velocity_per_day}</td>
                      <td style={{padding:'12px 14px',color:'#64748b',fontSize:12}}>{row.last_ordered}</td>
                      <td style={{padding:'12px 14px'}}>
                        <span style={{background:rc.bg,border:`1px solid ${rc.border}`,color:rc.color,borderRadius:8,padding:'4px 10px',fontSize:11,fontWeight:600}}>{rc.label}</span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Card>

        <div style={{marginTop:32,textAlign:'center',color:'#1e293b',fontSize:11,paddingBottom:24}}>
          Built with Kafka · PostgreSQL · Apache Airflow · FastAPI · React
        </div>
      </main>
    </>
  );
}