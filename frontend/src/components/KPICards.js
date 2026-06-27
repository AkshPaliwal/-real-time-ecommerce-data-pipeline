import React from 'react';

const fmt = (n) => new Intl.NumberFormat('en-IN', {
  style: 'currency', currency: 'INR', maximumFractionDigits: 0
}).format(n);

function KPICards({ data }) {
  const cards = [
    { label: 'Total Orders',       value: data.total_orders.toLocaleString(), color: '#3b82f6' },
    { label: 'Total Revenue',      value: fmt(data.total_revenue),            color: '#10b981' },
    { label: 'Total Customers',    value: data.total_customers.toLocaleString(), color: '#f59e0b' },
    { label: 'Avg Order Value',    value: fmt(data.average_order_value),      color: '#8b5cf6' },
  ];

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 24, marginBottom: 24 }}>
      {cards.map(c => (
        <div key={c.label} className="card" style={{ borderTop: `3px solid ${c.color}` }}>
          <p style={{ color: '#94a3b8', fontSize: 13, marginBottom: 8 }}>{c.label}</p>
          <p style={{ fontSize: 22, fontWeight: 700, color: '#f1f5f9' }}>{c.value}</p>
        </div>
      ))}
    </div>
  );
}

export default KPICards;
