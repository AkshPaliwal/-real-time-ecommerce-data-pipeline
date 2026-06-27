import React from 'react';

function TopProducts({ data }) {
  return (
    <div className="card">
      <h2>Top 10 Products</h2>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr style={{ borderBottom: '1px solid #334155' }}>
            {['#', 'Product', 'Qty Sold', 'Revenue'].map(h => (
              <th key={h} style={{ padding: '10px 12px', textAlign: 'left', color: '#64748b', fontSize: 13 }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={i} style={{ borderBottom: '1px solid #1e293b' }}>
              <td style={{ padding: '10px 12px', color: '#64748b' }}>{i + 1}</td>
              <td style={{ padding: '10px 12px', color: '#f1f5f9' }}>{row.product_name}</td>
              <td style={{ padding: '10px 12px', color: '#94a3b8' }}>{row.total_quantity.toLocaleString()}</td>
              <td style={{ padding: '10px 12px', color: '#10b981', fontWeight: 600 }}>
                ₹{row.total_revenue.toLocaleString('en-IN')}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default TopProducts;