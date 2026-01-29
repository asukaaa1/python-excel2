#!/usr/bin/env python3
"""
Excel to HTML Dashboard Converter
Converts Excel spreadsheets into interactive HTML dashboards with organized sections
"""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Any
import sys


class ExcelDashboardConverter:
    """Convert Excel files to interactive HTML dashboards"""
    
    def __init__(self, excel_file: str, output_file: str = "dashboard.html"):
        self.excel_file = excel_file
        self.output_file = output_file
        self.data_sections = {}
        
    def read_excel(self) -> Dict[str, pd.DataFrame]:
        """Read all sheets from Excel file"""
        try:
            excel_data = pd.read_excel(self.excel_file, sheet_name=None)
            return excel_data
        except Exception as e:
            print(f"Error reading Excel file: {e}")
            sys.exit(1)
    
    def detect_sections(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """Detect data sections in a sheet based on headers and structure"""
        sections = {}
        current_section = None
        section_start = None
        
        for idx, row in df.iterrows():
            # Check if row contains a section header (first cell has value, others mostly empty)
            first_cell = str(row.iloc[0]).strip()
            
            if first_cell and first_cell != 'nan':
                # Check if this looks like a header row (colored background or distinct text)
                if pd.notna(first_cell) and len(first_cell) > 0:
                    # Start new section
                    if current_section and section_start is not None:
                        sections[current_section] = {
                            'start': section_start,
                            'end': idx - 1,
                            'data': None
                        }
                    
                    current_section = first_cell
                    section_start = idx
        
        # Add last section
        if current_section and section_start is not None:
            sections[current_section] = {
                'start': section_start,
                'end': len(df) - 1,
                'data': None
            }
        
        return sections
    
    def organize_data(self, df: pd.DataFrame, sheet_name: str) -> List[Dict]:
        """Organize sheet data into logical sections"""
        sections = []
        
        # Try to find header rows (rows with text but mostly non-numeric data)
        header_indices = []
        for idx, row in df.iterrows():
            # Count non-null, non-numeric values
            text_count = sum(1 for val in row if pd.notna(val) and isinstance(val, str))
            if text_count >= 2:  # Likely a header row
                header_indices.append(idx)
        
        # Extract sections based on headers
        if header_indices:
            for i, header_idx in enumerate(header_indices):
                # Get section data
                next_header = header_indices[i + 1] if i + 1 < len(header_indices) else len(df)
                section_df = df.iloc[header_idx:next_header].copy()
                
                # Get section name from first row
                section_name = str(section_df.iloc[0, 0]) if not section_df.empty else f"Section {i+1}"
                
                # Use second row as column headers if it exists
                if len(section_df) > 1:
                    section_df.columns = section_df.iloc[0].fillna('')
                    section_df = section_df.iloc[1:].reset_index(drop=True)
                
                sections.append({
                    'name': section_name,
                    'data': section_df,
                    'sheet': sheet_name
                })
        else:
            # No clear sections, treat entire sheet as one section
            sections.append({
                'name': sheet_name,
                'data': df,
                'sheet': sheet_name
            })
        
        return sections
    
    def generate_html(self, all_data: Dict[str, pd.DataFrame]) -> str:
        """Generate complete HTML dashboard"""
        
        # Process all sheets
        all_sections = []
        for sheet_name, df in all_data.items():
            sections = self.organize_data(df, sheet_name)
            all_sections.extend(sections)
        
        # Convert sections to JSON for JavaScript
        sections_json = []
        for section in all_sections:
            df = section['data']
            # Convert DataFrame to records, handling NaN values
            records = df.fillna('').to_dict('records')
            columns = df.columns.tolist()
            
            sections_json.append({
                'name': section['name'],
                'sheet': section['sheet'],
                'columns': columns,
                'data': records
            })
        
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Excel Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Archivo:wght@300;400;600;700;900&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        :root {{
            --primary: #0a0e27;
            --secondary: #1a1f3a;
            --accent: #00d9ff;
            --accent-warm: #ff6b35;
            --text: #e8eaed;
            --text-dim: #a0a4b8;
            --border: #2d3349;
            --success: #00ff88;
            --warning: #ffb627;
            --error: #ff4757;
            --gradient-1: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            --gradient-2: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            --gradient-3: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            --shadow-sm: 0 2px 8px rgba(0, 0, 0, 0.4);
            --shadow-md: 0 4px 16px rgba(0, 0, 0, 0.5);
            --shadow-lg: 0 8px 32px rgba(0, 0, 0, 0.6);
        }}
        
        body {{
            font-family: 'Archivo', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--primary);
            color: var(--text);
            line-height: 1.6;
            overflow-x: hidden;
        }}
        
        .dashboard-header {{
            background: var(--secondary);
            border-bottom: 2px solid var(--accent);
            padding: 2rem 3rem;
            position: sticky;
            top: 0;
            z-index: 100;
            box-shadow: var(--shadow-lg);
        }}
        
        .dashboard-header h1 {{
            font-size: 2.5rem;
            font-weight: 900;
            letter-spacing: -0.03em;
            background: linear-gradient(135deg, var(--accent), var(--accent-warm));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 0.5rem;
        }}
        
        .dashboard-header p {{
            color: var(--text-dim);
            font-size: 0.95rem;
            letter-spacing: 0.02em;
        }}
        
        .controls {{
            display: flex;
            gap: 1rem;
            padding: 1.5rem 3rem;
            background: var(--secondary);
            border-bottom: 1px solid var(--border);
            flex-wrap: wrap;
            align-items: center;
        }}
        
        .search-box {{
            flex: 1;
            min-width: 300px;
            position: relative;
        }}
        
        .search-box input {{
            width: 100%;
            padding: 0.75rem 1rem 0.75rem 2.5rem;
            background: var(--primary);
            border: 2px solid var(--border);
            border-radius: 8px;
            color: var(--text);
            font-size: 0.95rem;
            font-family: inherit;
            transition: all 0.3s ease;
        }}
        
        .search-box input:focus {{
            outline: none;
            border-color: var(--accent);
            box-shadow: 0 0 0 3px rgba(0, 217, 255, 0.1);
        }}
        
        .search-icon {{
            position: absolute;
            left: 0.75rem;
            top: 50%;
            transform: translateY(-50%);
            color: var(--text-dim);
            font-size: 1.1rem;
        }}
        
        .filter-group {{
            display: flex;
            gap: 0.5rem;
            flex-wrap: wrap;
        }}
        
        .filter-btn {{
            padding: 0.6rem 1.2rem;
            background: var(--primary);
            border: 2px solid var(--border);
            border-radius: 8px;
            color: var(--text);
            font-size: 0.85rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        
        .filter-btn:hover {{
            border-color: var(--accent);
            background: rgba(0, 217, 255, 0.1);
        }}
        
        .filter-btn.active {{
            background: var(--accent);
            color: var(--primary);
            border-color: var(--accent);
        }}
        
        .dashboard-container {{
            max-width: 1800px;
            margin: 0 auto;
            padding: 3rem;
        }}
        
        .section-grid {{
            display: grid;
            gap: 2rem;
            animation: fadeIn 0.6s ease;
        }}
        
        @keyframes fadeIn {{
            from {{
                opacity: 0;
                transform: translateY(20px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}
        
        .section-card {{
            background: var(--secondary);
            border: 1px solid var(--border);
            border-radius: 16px;
            overflow: hidden;
            box-shadow: var(--shadow-md);
            transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        }}
        
        .section-card:hover {{
            transform: translateY(-4px);
            box-shadow: var(--shadow-lg);
            border-color: var(--accent);
        }}
        
        .section-header {{
            padding: 1.5rem 2rem;
            background: linear-gradient(135deg, rgba(0, 217, 255, 0.1), rgba(255, 107, 53, 0.1));
            border-bottom: 1px solid var(--border);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .section-title {{
            font-size: 1.3rem;
            font-weight: 700;
            letter-spacing: -0.02em;
        }}
        
        .section-badge {{
            background: var(--accent);
            color: var(--primary);
            padding: 0.3rem 0.8rem;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        
        .table-container {{
            overflow-x: auto;
            padding: 1.5rem 2rem;
        }}
        
        .data-table {{
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
        }}
        
        .data-table thead {{
            background: rgba(0, 217, 255, 0.05);
        }}
        
        .data-table th {{
            padding: 1rem;
            text-align: left;
            font-weight: 700;
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            color: var(--accent);
            border-bottom: 2px solid var(--accent);
            white-space: nowrap;
        }}
        
        .data-table td {{
            padding: 0.9rem 1rem;
            border-bottom: 1px solid var(--border);
            font-size: 0.9rem;
            color: var(--text);
            font-family: 'JetBrains Mono', monospace;
        }}
        
        .data-table tbody tr {{
            transition: background-color 0.2s ease;
        }}
        
        .data-table tbody tr:hover {{
            background: rgba(0, 217, 255, 0.05);
        }}
        
        .data-table tbody tr:last-child td {{
            border-bottom: none;
        }}
        
        /* Number formatting */
        .data-table td:not(:first-child) {{
            text-align: right;
            font-variant-numeric: tabular-nums;
        }}
        
        .metric-card {{
            background: linear-gradient(135deg, var(--secondary) 0%, rgba(26, 31, 58, 0.6) 100%);
            padding: 1.5rem;
            border-radius: 12px;
            border: 1px solid var(--border);
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }}
        
        .metric-label {{
            font-size: 0.8rem;
            color: var(--text-dim);
            text-transform: uppercase;
            letter-spacing: 0.1em;
            font-weight: 600;
        }}
        
        .metric-value {{
            font-size: 2rem;
            font-weight: 900;
            font-family: 'JetBrains Mono', monospace;
            background: linear-gradient(135deg, var(--accent), var(--accent-warm));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        
        .empty-state {{
            text-align: center;
            padding: 4rem 2rem;
            color: var(--text-dim);
        }}
        
        .empty-state svg {{
            width: 80px;
            height: 80px;
            margin-bottom: 1rem;
            opacity: 0.3;
        }}
        
        @media (max-width: 768px) {{
            .dashboard-header {{
                padding: 1.5rem;
            }}
            
            .dashboard-header h1 {{
                font-size: 1.8rem;
            }}
            
            .controls {{
                padding: 1rem 1.5rem;
            }}
            
            .dashboard-container {{
                padding: 1.5rem;
            }}
            
            .section-header {{
                flex-direction: column;
                align-items: flex-start;
                gap: 0.5rem;
            }}
            
            .table-container {{
                padding: 1rem;
            }}
        }}
        
        .no-results {{
            text-align: center;
            padding: 3rem;
            color: var(--text-dim);
        }}
    </style>
</head>
<body>
    <div class="dashboard-header">
        <h1>📊 Excel Dashboard</h1>
        <p>Interactive data visualization and analysis</p>
    </div>
    
    <div class="controls">
        <div class="search-box">
            <span class="search-icon">🔍</span>
            <input type="text" id="searchInput" placeholder="Search across all sections...">
        </div>
        <div class="filter-group" id="sheetFilters"></div>
    </div>
    
    <div class="dashboard-container">
        <div class="section-grid" id="sectionsContainer"></div>
    </div>

    <script>
        const sectionsData = {json.dumps(sections_json, ensure_ascii=False, indent=2)};
        
        let currentFilter = 'all';
        let searchTerm = '';
        
        function renderSections() {{
            const container = document.getElementById('sectionsContainer');
            
            // Filter sections
            let filteredSections = sectionsData.filter(section => {{
                const matchesFilter = currentFilter === 'all' || section.sheet === currentFilter;
                const matchesSearch = !searchTerm || 
                    section.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                    section.data.some(row => 
                        Object.values(row).some(val => 
                            String(val).toLowerCase().includes(searchTerm.toLowerCase())
                        )
                    );
                return matchesFilter && matchesSearch;
            }});
            
            if (filteredSections.length === 0) {{
                container.innerHTML = `
                    <div class="empty-state">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <circle cx="12" cy="12" r="10"/>
                            <line x1="12" y1="8" x2="12" y2="12"/>
                            <line x1="12" y1="16" x2="12.01" y2="16"/>
                        </svg>
                        <h3>No results found</h3>
                        <p>Try adjusting your search or filters</p>
                    </div>
                `;
                return;
            }}
            
            container.innerHTML = filteredSections.map(section => {{
                const columns = section.columns;
                const rows = section.data;
                
                return `
                    <div class="section-card">
                        <div class="section-header">
                            <h2 class="section-title">${{section.name}}</h2>
                            <span class="section-badge">${{section.sheet}}</span>
                        </div>
                        <div class="table-container">
                            <table class="data-table">
                                <thead>
                                    <tr>
                                        ${{columns.map(col => `<th>${{col}}</th>`).join('')}}
                                    </tr>
                                </thead>
                                <tbody>
                                    ${{rows.map(row => `
                                        <tr>
                                            ${{columns.map(col => `<td>${{formatValue(row[col])}}</td>`).join('')}}
                                        </tr>
                                    `).join('')}}
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;
            }}).join('');
        }}
        
        function formatValue(value) {{
            if (value === null || value === undefined || value === '') {{
                return '-';
            }}
            
            // Check if it's a number
            if (!isNaN(value) && typeof value === 'number') {{
                // Format with thousand separators
                return value.toLocaleString('pt-BR', {{maximumFractionDigits: 2}});
            }}
            
            // Check if it's a percentage string
            if (typeof value === 'string' && value.includes('%')) {{
                return value;
            }}
            
            // Check if it's a currency string (starts with R$)
            if (typeof value === 'string' && value.includes('R$')) {{
                return value;
            }}
            
            return value;
        }}
        
        function initializeFilters() {{
            const sheets = [...new Set(sectionsData.map(s => s.sheet))];
            const filtersContainer = document.getElementById('sheetFilters');
            
            filtersContainer.innerHTML = `
                <button class="filter-btn active" data-filter="all">All Sheets</button>
                ${{sheets.map(sheet => 
                    `<button class="filter-btn" data-filter="${{sheet}}">${{sheet}}</button>`
                ).join('')}}
            `;
            
            filtersContainer.querySelectorAll('.filter-btn').forEach(btn => {{
                btn.addEventListener('click', () => {{
                    filtersContainer.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    currentFilter = btn.dataset.filter;
                    renderSections();
                }});
            }});
        }}
        
        document.getElementById('searchInput').addEventListener('input', (e) => {{
            searchTerm = e.target.value;
            renderSections();
        }});
        
        // Initialize
        initializeFilters();
        renderSections();
    </script>
</body>
</html>"""
        
        return html_content
    
    def convert(self):
        """Main conversion process"""
        print(f"Reading Excel file: {self.excel_file}")
        excel_data = self.read_excel()
        
        print(f"Found {len(excel_data)} sheet(s)")
        for sheet_name in excel_data.keys():
            print(f"  - {sheet_name}")
        
        print("Generating HTML dashboard...")
        html = self.generate_html(excel_data)
        
        print(f"Writing output to: {self.output_file}")
        with open(self.output_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print("✅ Dashboard created successfully!")
        return self.output_file


def main():
    if len(sys.argv) < 2:
        print("Usage: python excel_to_dashboard.py <excel_file> [output_file]")
        print("\nExample:")
        print("  python excel_to_dashboard.py data.xlsx")
        print("  python excel_to_dashboard.py data.xlsx custom_dashboard.html")
        sys.exit(1)
    
    excel_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "dashboard.html"
    
    if not Path(excel_file).exists():
        print(f"Error: File '{excel_file}' not found")
        sys.exit(1)
    
    converter = ExcelDashboardConverter(excel_file, output_file)
    converter.convert()


if __name__ == "__main__":
    main()