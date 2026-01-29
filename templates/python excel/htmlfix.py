"""
Script to fix CSS curly braces in HTML template files for Python .format()
This escapes { to {{ and } to }} only within <style> tags
"""

import re
from pathlib import Path
import sys

def escape_css_braces(content):
    """Escape CSS curly braces in style blocks"""
    
    def escape_style_block(match):
        style_content = match.group(1)
        # Double all curly braces in CSS
        escaped = style_content.replace('{', '{{').replace('}', '}}')
        return f'<style>{escaped}</style>'
    
    # Replace all style blocks
    content = re.sub(r'<style>(.*?)</style>', escape_style_block, content, flags=re.DOTALL)
    
    return content

def process_file(file_path):
    """Process a single HTML file"""
    try:
        print(f"Processing: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check if file already has escaped braces
        if '{{' in content and '}}' in content:
            print(f"  ⚠️  File appears to already have escaped braces, skipping")
            return False
        
        # Escape the braces
        fixed_content = escape_css_braces(content)
        
        # Create backup
        backup_path = file_path.parent / f"{file_path.stem}_backup{file_path.suffix}"
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✅ Backup created: {backup_path.name}")
        
        # Write fixed content
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        print(f"  ✅ Fixed: {file_path.name}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error processing {file_path}: {e}")
        return False

def main():
    """Main function to process all template files"""
    print("=" * 60)
    print("HTML Template CSS Brace Fixer")
    print("=" * 60)
    print()
    
    # Get templates directory
    if len(sys.argv) > 1:
        templates_dir = Path(sys.argv[1])
    else:
        templates_dir = Path("templates")
    
    if not templates_dir.exists():
        print(f"❌ Templates directory not found: {templates_dir}")
        print("\nUsage:")
        print("  python fix_template_braces.py [templates_directory]")
        print("\nExample:")
        print("  python fix_template_braces.py templates")
        return
    
    # Find all HTML files
    html_files = list(templates_dir.glob("*.html"))
    
    if not html_files:
        print(f"❌ No HTML files found in: {templates_dir}")
        return
    
    print(f"Found {len(html_files)} HTML file(s) in {templates_dir}")
    print()
    
    # Process each file
    fixed_count = 0
    for html_file in html_files:
        if process_file(html_file):
            fixed_count += 1
        print()
    
    print("=" * 60)
    print(f"✅ Complete! Fixed {fixed_count} file(s)")
    print("=" * 60)
    print()
    print("💡 Backup files created with '_backup' suffix")
    print("   You can delete them once you verify everything works!")

if __name__ == "__main__":
    main()