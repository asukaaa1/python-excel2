"""
Multi-Restaurant Analytics Dashboard System - FIXED VERSION
Now properly handles sheets without numeric data
"""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys
import os
import numpy as np
import shutil
import hashlib


class MultiRestaurantDashboard:
    """Create a multi-restaurant dashboard system with improved error handling"""
    
    def __init__(self, output_folder: str = "dashboard_output"):
        self.output_folder = Path(output_folder)
        self.restaurants = []
    
    def load_template(self, template_name: str) -> str:
        """Load an HTML template file"""
        template_path = Path("templates") / template_name
        if not template_path.exists():
            raise FileNotFoundError(f"Template not found: {template_path}")
        
        with open(template_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def setup_output_folder(self):
        """Create output folder structure"""
        if self.output_folder.exists():
            shutil.rmtree(self.output_folder)
        self.output_folder.mkdir(parents=True, exist_ok=True)
        (self.output_folder / "restaurants").mkdir(exist_ok=True)
        
    def add_restaurant(self, excel_file: str, name: str = None, manager: str = "Gerente", platforms: List[str] = None):
        """Add a restaurant from an Excel file"""
        file_path = Path(excel_file)
        if not file_path.exists():
            print(f"Warning: File {excel_file} not found, skipping")
            return
            
        # Use filename as name if not provided
        if not name:
            name = file_path.stem.replace('_', ' ').replace('-', ' ').title()
        
        # Generate ID from name (to allow same file for different restaurants)
        restaurant_id = hashlib.md5(name.encode()).hexdigest()[:8]
        
        # Detect platforms from sheet names
        if not platforms:
            try:
                xl = pd.ExcelFile(excel_file, engine='openpyxl')
                platforms = [s for s in xl.sheet_names if self._has_data(xl, s)][:3]
            except:
                platforms = ['iFood']
        
        self.restaurants.append({
            'id': restaurant_id,
            'name': name,
            'file': str(file_path),
            'manager': manager,
            'platforms': platforms
        })
        
    def _has_data(self, xl: pd.ExcelFile, sheet_name: str) -> bool:
        """Check if a sheet has actual data"""
        try:
            df = pd.read_excel(xl, sheet_name=sheet_name, header=1)
            df = df.dropna(axis=1, how='all').dropna(how='all')
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            return any(df[col].notna().sum() > 0 for col in numeric_cols)
        except:
            return False
    
    def read_excel(self, excel_file: str) -> Dict[str, pd.DataFrame]:
        """Read all sheets with smart header detection AND date sorting"""
        try:
            xl = pd.ExcelFile(excel_file, engine='openpyxl')
            cleaned_data = {}
            
            for sheet_name in xl.sheet_names:
                print(f"   📄 Reading sheet: {sheet_name}")
                
                # Read with no header first to detect header row
                df_raw = pd.read_excel(xl, sheet_name=sheet_name, header=None)
                header_row = self._find_header_row(df_raw)
                
                # Read again with correct header
                df = pd.read_excel(xl, sheet_name=sheet_name, header=header_row)
                
                # Clean up
                df = df.dropna(axis=1, how='all')  # Remove empty columns
                df = df.dropna(how='all')  # Remove empty rows
                df = df.loc[:, ~df.columns.astype(str).str.contains('^Unnamed')]
                
                # Reset index
                df = df.reset_index(drop=True)
                
                # Sort by date if possible
                label_col = self.find_label_column(df)
                if label_col:
                    try:
                        df['_sort_key'] = pd.to_datetime(df[label_col], dayfirst=True, errors='coerce')
                        df = df.sort_values('_sort_key').drop(columns=['_sort_key'])
                        df = df.reset_index(drop=True)
                        print(f"      ✓ Sorted by date column: {label_col}")
                    except Exception as e:
                        print(f"      ⚠ Could not sort by date: {e}")
                
                # Convert numeric columns
                for col in df.columns:
                    if col != label_col:
                        df[col] = pd.to_numeric(df[col], errors='ignore')
                
                # Debug output
                numeric_count = len(df.select_dtypes(include=[np.number]).columns)
                print(f"      ✓ Columns: {len(df.columns)}, Rows: {len(df)}")
                print(f"      ✓ Numeric columns: {numeric_count}")
                
                # FIXED: Only include sheets with actual numeric data
                if len(df) > 0 and len(df.columns) > 1 and numeric_count > 0:
                    cleaned_data[sheet_name] = df
                else:
                    print(f"      ⚠ Skipped (no numeric data)")
            
            return cleaned_data
            
        except Exception as e:
            print(f"   ❌ Error reading Excel file: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    def _find_header_row(self, df: pd.DataFrame) -> int:
        """Find the row that contains column headers"""
        best_row = 0
        best_score = 0
        
        for i in range(min(5, len(df))):
            row = df.iloc[i]
            non_null = row.notna().sum()
            text_count = sum(1 for v in row if isinstance(v, str) and v.strip())
            score = non_null + text_count
            
            if score > best_score:
                best_score = score
                best_row = i
        
        return best_row
    
    def generate_admin_page(self, restaurants_data: List[Dict]) -> str:
        template = self.load_template('admin.html')
        restaurants_json = json.dumps(restaurants_data, ensure_ascii=False)
        
        return template.format(restaurants_json=restaurants_json)
    
    def generate_login_page(self) -> str:
        template = self.load_template('login.html')
        return template
    
    def find_label_column(self, df: pd.DataFrame) -> Optional[str]:
        """Find the best column to use as labels"""
        label_keywords = ['mês', 'mes', 'month', 'data', 'date', 'dia', 'day', 
                         'período', 'periodo', 'period', 'semana', 'week']
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if any(keyword in col_lower for keyword in label_keywords):
                return col
        
        # Fallback to first non-numeric column
        for col in df.columns:
            if df[col].dtype == 'object':
                return col
        
        return df.columns[0] if len(df.columns) > 0 else None
    
    def get_numeric_columns(self, df: pd.DataFrame, exclude_col: Optional[str] = None) -> List[str]:
        """Get all numeric columns with actual data"""
        numeric_cols = []
        for col in df.select_dtypes(include=[np.number]).columns:
            if col != exclude_col and df[col].notna().sum() > 0:
                numeric_cols.append(col)
        return numeric_cols
    
    def extract_summary_metrics(self, all_data: Dict[str, pd.DataFrame]) -> Dict:
        """Extract summary metrics for the restaurant list view"""
        if not all_data:
            return {'vendas': 0, 'ticket_medio': 0, 'valor_bruto': 0, 'liquido': 0, 'trends': {}}
        
        first_sheet = list(all_data.values())[0]
        label_col = self.find_label_column(first_sheet)
        numeric_cols = self.get_numeric_columns(first_sheet, label_col)
        
        metrics = {'vendas': 0, 'ticket_medio': 0, 'valor_bruto': 0, 'liquido': 0, 'trends': {}}
        
        col_mapping = {
            'vendas': ['vendas', 'sales', 'pedidos', 'orders'],
            'ticket_medio': ['ticket médio', 'ticket medio', 'average ticket', 'ticket'],
            'valor_bruto': ['valor bruto', 'bruto', 'gross', 'faturamento'],
            'liquido': ['líquido', 'liquido', 'net', 'receita líquida']
        }
        
        for metric_key, keywords in col_mapping.items():
            for col in numeric_cols:
                col_lower = str(col).lower()
                if any(kw in col_lower for kw in keywords):
                    col_data = first_sheet[col].dropna()
                    if len(col_data) > 0:
                        metrics[metric_key] = float(col_data.iloc[-1])
                        
                        # Calculate trend
                        if len(col_data) >= 2:
                            prev = col_data.iloc[-2]
                            curr = col_data.iloc[-1]
                            if prev > 0:
                                metrics['trends'][metric_key] = ((curr - prev) / prev) * 100
                            else:
                                metrics['trends'][metric_key] = 0
                        else:
                            metrics['trends'][metric_key] = 0
                    break
        
        # Get last period
        if label_col and label_col in first_sheet.columns:
            last_period = first_sheet[label_col].dropna().iloc[-1] if len(first_sheet[label_col].dropna()) > 0 else "N/A"
            metrics['last_period'] = str(last_period)
        else:
            metrics['last_period'] = "N/A"
        
        return metrics
    
    def generate_main_page(self, restaurants_data: List[Dict]) -> str:
        """Generate the main page HTML"""
        template = self.load_template("index.html")
        restaurants_json = json.dumps(restaurants_data, ensure_ascii=False)
        
        return template.format(restaurants_json=restaurants_json)

    def prepare_chart_data_for_sheet(self, df: pd.DataFrame) -> Dict:
        """Prepare chart data from a DataFrame - FIXED to handle no numeric columns"""
        
        # Find the label column (date/period column)
        label_col = self.find_label_column(df)
        if not label_col:
            print(f"      ⚠ No label column found")
            return {'labels': [], 'datasets': {}, 'columns': []}
        
        # Get labels (dates/periods) as strings
        labels = [str(x) if pd.notna(x) else '' for x in df[label_col].tolist()]
        
        # Get all numeric columns
        numeric_cols = self.get_numeric_columns(df, label_col)
        
        if not numeric_cols:
            print(f"      ⚠ No numeric columns found - sheet will be hidden")
            return {'labels': [], 'datasets': {}, 'columns': []}
        
        # Color palette for charts
        colors = [
            '#ef4444', '#f97316', '#eab308', '#22c55e', '#14b8a6',
            '#3b82f6', '#6366f1', '#a855f7', '#ec4899', '#64748b'
        ]
        
        # Prepare datasets dictionary
        datasets = {}
        
        for idx, col in enumerate(numeric_cols):
            # Get values, replacing NaN with 0
            values = [float(x) if pd.notna(x) and np.isfinite(x) else 0 for x in df[col].tolist()]
            
            color = colors[idx % len(colors)]
            
            datasets[col] = {
                'label': col,
                'data': values,
                'borderColor': color,
                'backgroundColor': color + '20'  # 20 = 12.5% opacity in hex
            }
        
        print(f"      ✓ Created chart data: {len(labels)} labels, {len(datasets)} datasets")
        
        return {
            'labels': labels,
            'datasets': datasets,
            'columns': numeric_cols
        }

    def prepare_table_data(self, df: pd.DataFrame) -> Dict:
        """Prepare table data with proper column info"""
        
        label_col = self.find_label_column(df)
        
        # Convert DataFrame to records, handling NaN values
        records = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                val = row[col]
                if pd.isna(val):
                    record[col] = None
                elif isinstance(val, (int, float, np.integer, np.floating)):
                    if np.isfinite(val):
                        record[col] = float(val)
                    else:
                        record[col] = None
                else:
                    record[col] = str(val)
            records.append(record)
        
        return {
            'name': df.name if hasattr(df, 'name') else 'Sheet',
            'columns': df.columns.tolist(),
            'data': records
        }

    def generate_restaurant_dashboard(self, restaurant: Dict, all_data: Dict[str, pd.DataFrame]) -> str:
        """Generate dashboard HTML with improved data handling"""
        
        template = self.load_template("dashboard.html")
        
        # Prepare chart data and tables for each sheet
        all_sheets_chart_data = {}
        tables_data = []
        valid_sheets = []
        
        for sheet_name, df in all_data.items():
            print(f"   📊 Preparing chart data for: {sheet_name}")
            
            # Prepare chart data for this sheet
            chart_data = self.prepare_chart_data_for_sheet(df)
            
            # FIXED: Only include sheets that have actual chart data
            if chart_data['columns']:  # Has numeric columns
                all_sheets_chart_data[sheet_name] = chart_data
                valid_sheets.append(sheet_name)
                
                # Prepare table data
                table_data = self.prepare_table_data(df)
                table_data['name'] = sheet_name
                tables_data.append(table_data)
            else:
                print(f"      ⚠ Sheet '{sheet_name}' skipped - no numeric data")
        
        # FIXED: Use only valid sheets
        sheet_names = valid_sheets
        
        # Convert to JSON with proper handling
        all_sheets_json = json.dumps(all_sheets_chart_data, ensure_ascii=False, cls=NumpyEncoder)
        tables_json = json.dumps(tables_data, ensure_ascii=False, cls=NumpyEncoder)
        sheet_names_json = json.dumps(sheet_names, ensure_ascii=False)
        
        # Generate platforms HTML
        platforms = restaurant.get('platforms', [])
        platforms_html = ''.join(f'<span class="platform-tag">{p}</span>' for p in platforms)
        
        # Inject data using .format()
        html = template.format(
            restaurant_name=restaurant['name'],
            restaurant_manager=restaurant.get('manager', 'Gerente'),
            platforms_html=platforms_html,
            all_sheets_chart_data=all_sheets_json,
            tables_data=tables_json,
            sheet_names=sheet_names_json
        )
        
        return html

    def build(self):
        """Build the complete dashboard system"""
        print("🗂️ Building Multi-Restaurant Dashboard System")
        print("=" * 50)
        
        self.setup_output_folder()
        
        restaurants_with_data = []
        
        for restaurant in self.restaurants:
            print(f"\n📊 Processing: {restaurant['name']}")
            
            all_data = self.read_excel(restaurant['file'])
            
            if not all_data:
                print(f"   ⚠️ No valid sheets with numeric data found, skipping")
                continue
            
            metrics = self.extract_summary_metrics(all_data)
            restaurant['metrics'] = metrics
            
            # Generate individual dashboard
            dashboard_html = self.generate_restaurant_dashboard(restaurant, all_data)
            dashboard_path = self.output_folder / "restaurants" / f"{restaurant['id']}.html"
            
            with open(dashboard_path, 'w', encoding='utf-8') as f:
                f.write(dashboard_html)
            
            print(f"   ✅ Dashboard created: {dashboard_path.name}")
            restaurants_with_data.append(restaurant)
        
        # Generate login page
        print(f"\n🔐 Generating login page...")
        login_html = self.generate_login_page()
        login_path = self.output_folder / "login.html"
        with open(login_path, 'w', encoding='utf-8') as f:
            f.write(login_html)
        
        # Generate main page with embedded restaurant data
        print(f"\n📋 Generating main restaurant list...")
        main_html = self.generate_main_page(restaurants_with_data)
        main_path = self.output_folder / "index.html"
        with open(main_path, 'w', encoding='utf-8') as f:
            f.write(main_html)
        
        # Generate admin page
        print(f"\n⚙️ Generating admin page...")
        admin_html = self.generate_admin_page(restaurants_with_data)
        admin_path = self.output_folder / "admin.html"
        with open(admin_path, 'w', encoding='utf-8') as f:
            f.write(admin_html)
        
        print(f"\n✅ Dashboard system created successfully!")
        print(f"   📁 Output folder: {self.output_folder}")
        print(f"   🔐 Login page: login.html")
        print(f"   🏠 Main page: index.html")
        print(f"   ⚙️ Admin page: admin.html")
        print(f"   🍽️ Restaurants: {len(restaurants_with_data)}")
        
        return str(login_path)


class NumpyEncoder(json.JSONEncoder):
    """Handle numpy types in JSON encoding"""
    def default(self, obj):
        if isinstance(obj, (np.integer, np.floating)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def main():
    if len(sys.argv) < 2:
        print("Usage: python multi_restaurant_dashboard.py <excel_file1> [excel_file2] ... [--output folder]")
        print("\nExamples:")
        print("  python multi_restaurant_dashboard.py restaurant1.xlsx restaurant2.xlsx")
        print("  python multi_restaurant_dashboard.py data/*.xlsx --output my_dashboards")
        print("\nYou can also specify restaurant names:")
        print("  python multi_restaurant_dashboard.py restaurant1.xlsx:\"American Taste\" restaurant2.xlsx:\"Amo Pastel\"")
        sys.exit(1)
    
    # Parse arguments
    output_folder = "dashboard_output"
    files = []
    
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == "--output" and i + 1 < len(sys.argv):
            output_folder = sys.argv[i + 1]
            i += 2
        else:
            files.append(arg)
            i += 1
    
    # Create dashboard system
    dashboard = MultiRestaurantDashboard(output_folder=output_folder)
    
    # Add restaurants
    for file_spec in files:
        if ':' in file_spec:
            file_path, name = file_spec.rsplit(':', 1)
        else:
            file_path = file_spec
            name = None
        
        dashboard.add_restaurant(file_path, name=name)
    
    # Build
    dashboard.build()


if __name__ == "__main__":
    main()