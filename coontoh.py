"""
Monitoring Routes Module
Flask routes for monitoring dashboard with dynamic table access
"""

from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from functools import wraps
import pymysql

# Create Blueprint
monitoring_bp = Blueprint('monitoring', __name__, url_prefix='/monitoring')

def login_required(f):
    """Decorator to require login"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated_function

def get_all_billing_tables():
    """Get all billing tables from database"""
    return ['billing']

def get_user_info(username):
    """Get user information from tb_user"""
    conn = get_db_connection2()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT unitup, nama_ulp 
            FROM tb_user 
            WHERE username = %s
        """, (username,))
        
        return cursor.fetchone()
    finally:
        cursor.close()
        conn.close()

def get_latest_blth():
    """Get the latest BLTH from billing table"""
    conn = get_db_connection2()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT MAX(BLTH) as latest_blth 
            FROM billing 
            WHERE BLTH IS NOT NULL
        """)
        result = cursor.fetchone()
        return result['latest_blth'] if result else None
    finally:
        cursor.close()
        conn.close()

def get_available_blth_list():
    """Get list of available BLTH values"""
    conn = get_db_connection2()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT DISTINCT BLTH 
            FROM billing 
            WHERE BLTH IS NOT NULL 
            ORDER BY BLTH DESC
        """)
        return [row['BLTH'] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

def get_available_unitup_list():
    """Get list of available UNITUP values"""
    conn = get_db_connection2()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT DISTINCT u.unitup, u.nama_ulp 
            FROM tb_user u
            INNER JOIN billing b ON u.unitup = b.UNITUP
            WHERE u.unitup IS NOT NULL 
            AND u.unitup != 'UP3'
            ORDER BY u.unitup
        """)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

def get_user_unitup_filter(username, override_unitup=None):
    """Get UNITUP filter for user"""
    user_info = get_user_info(username)
    
    if not user_info:
        return None
    
    unitup = user_info.get('unitup', '')
    nama_ulp = user_info.get('nama_ulp', '')
    
    # Jika UP3 atau Administrator
    if unitup == 'UP3' or 'Administrator' in str(nama_ulp) or 'UP3' in str(nama_ulp).upper():
        # Jika ada override dari filter, gunakan itu
        return override_unitup if override_unitup else None
    
    # Return unitup user untuk filter WHERE clause
    return unitup

def get_user_tables(username, selected_unitup=None):
    """Get accessible tables for user based on their unitup"""
    user_info = get_user_info(username)
    
    if not user_info:
        print(f"[DEBUG] User info not found for: {username}")
        return []
    
    unitup = user_info.get('unitup', '')
    nama_ulp = user_info.get('nama_ulp', '')
    
    print(f"[DEBUG] User: {username}, UnitUp: {unitup}, Nama ULP: {nama_ulp}")
    
    # Check if admin
    is_admin = unitup == 'UP3' or 'Administrator' in str(nama_ulp) or 'UP3' in str(nama_ulp).upper()
    
    if is_admin:
        # Admin: Get all ULPs or selected one
        if selected_unitup:
            conn = get_db_connection2()
            cursor = conn.cursor()
            cursor.execute("SELECT nama_ulp FROM tb_user WHERE unitup = %s LIMIT 1", (selected_unitup,))
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            ulp_name = result['nama_ulp'].upper() if result else f'UNITUP {selected_unitup}'
            return [('billing', ulp_name, selected_unitup)]
        else:
            # Get all ULPs
            conn = get_db_connection2()
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT UNITUP FROM billing WHERE UNITUP IS NOT NULL ORDER BY UNITUP")
            unitup_list = [row['UNITUP'] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            tables = []
            for up in unitup_list:
                conn = get_db_connection2()
                cursor = conn.cursor()
                cursor.execute("SELECT nama_ulp FROM tb_user WHERE unitup = %s LIMIT 1", (up,))
                result = cursor.fetchone()
                cursor.close()
                conn.close()
                
                ulp_name = result['nama_ulp'].upper() if result else f'UNITUP {up}'
                tables.append(('billing', ulp_name, up))
            
            return tables
    else:
        # Regular user: Only their ULP
        display_name = nama_ulp.upper() if nama_ulp else 'BILLING'
        return [('billing', display_name, unitup)]

def validate_table_access(username, table):
    """Validate if user has access to table"""
    user_info = get_user_info(username)
    
    if not user_info:
        return False
    
    unitup = user_info.get('unitup', '')
    nama_ulp = user_info.get('nama_ulp', '')
    
    # Administrator UP3 bisa akses semua
    if unitup == 'UP3' or 'Administrator' in str(nama_ulp) or 'UP3' in str(nama_ulp).upper():
        return True
    
    return True  # For single billing table, allow access

# ==================== MAIN DASHBOARD ROUTE ====================

@monitoring_bp.route("/dashboard")
@login_required
def dashboard_monitoring():
    """Main monitoring dashboard with UNITUP and BLTH filters"""
    from .monitoring_service import MonitoringService
    
    user = session.get('username', '')
    user_info = get_user_info(user)
    
    if not user_info:
        print(f"[WARNING] User {user} info not found")
        return render_template("monitoring_dashboard.html",
            tables=[],
            recap_all={}, 
            total_all={}, 
            summary_all={'NAIK': {'jumlah': 0, 'persentase': 0}, 
                        'TURUN': {'jumlah': 0, 'persentase': 0},
                        'DIV/NA': {'jumlah': 0, 'persentase': 0},
                        'AMAN': {'jumlah': 0, 'persentase': 0}},
            recap_marking={}, 
            total_marking={}, 
            summary_marking={'NAIK': {'jumlah': 0, 'persentase': 0}, 
                            'TURUN': {'jumlah': 0, 'persentase': 0},
                            'DIV/NA': {'jumlah': 0, 'persentase': 0},
                            'AMAN': {'jumlah': 0, 'persentase': 0}},
            per_table_recap={},
            per_table_marking={},
            pivot_tables_all={},
            pivot_tables_marking={},
            pivot_tables_dlpd={},
            pivot_tables_koreksi={},
            pivot_tables_ganda={},
            available_unitup=[],
            available_blth=[],
            selected_unitup=None,
            selected_blth=None,
            error_message="User info tidak ditemukan.",
            username=user
        )
    
    unitup = user_info.get('unitup', '')
    nama_ulp = user_info.get('nama_ulp', '')
    
    # Check if admin
    is_admin = unitup == 'UP3' or 'Administrator' in str(nama_ulp) or 'UP3' in str(nama_ulp).upper()
    
    # ✅ PERUBAHAN: Menggunakan logika redirect dari Kode 1
    # Get filter parameters from request
    selected_unitup = request.args.get('unitup')
    selected_blth = request.args.get('blth')  # None if not present

    # If blth param is missing entirely, redirect to same route with default blth
    if 'blth' not in request.args:
        default_blth = get_latest_blth()  # ambil BLTH terbaru
        # preserve unitup in URL (could be None or empty string)
        return redirect(url_for('.dashboard_monitoring', unitup=selected_unitup or '', blth=default_blth))
    
    # Get available options for filters
    available_blth = get_available_blth_list()
    available_unitup = get_available_unitup_list() if is_admin else []
    
    # Get list of ULPs to display
    tables = get_user_tables(user, selected_unitup)
    
    if not tables:
        print(f"[WARNING] No tables for user {user}")
        return render_template("monitoring_dashboard.html",
            tables=[],
            recap_all={}, 
            total_all={}, 
            summary_all={'NAIK': {'jumlah': 0, 'persentase': 0}, 
                        'TURUN': {'jumlah': 0, 'persentase': 0},
                        'DIV/NA': {'jumlah': 0, 'persentase': 0},
                        'AMAN': {'jumlah': 0, 'persentase': 0}},
            recap_marking={}, 
            total_marking={}, 
            summary_marking={'NAIK': {'jumlah': 0, 'persentase': 0}, 
                            'TURUN': {'jumlah': 0, 'persentase': 0},
                            'DIV/NA': {'jumlah': 0, 'persentase': 0},
                            'AMAN': {'jumlah': 0, 'persentase': 0}},
            per_table_recap={},
            per_table_marking={},
            pivot_tables_all={},
            pivot_tables_marking={},
            pivot_tables_dlpd={},
            pivot_tables_koreksi={},
            pivot_tables_ganda={},
            available_unitup=available_unitup,
            available_blth=available_blth,
            selected_unitup=selected_unitup,
            selected_blth=selected_blth,
            error_message="Tidak ada data ULP yang tersedia.",
            username=user
        )
    
    print(f"[INFO] User {user} accessing ULPs: {[t[1] for t in tables]}, BLTH: {selected_blth}")
    
    # Prepare data structures
    all_recap_data = []
    all_marking_data = []
    all_ket_data = []
    all_ket_marking_data = []
    
    per_table_recap = {}
    per_table_marking = {}
    pivot_tables_all = {}
    pivot_tables_marking = {}
    pivot_tables_dlpd = {}
    pivot_tables_ganda = {}
    pivot_tables_koreksi = {}
    
    # Generate data for each ULP with BLTH filter
    for tbl, name, up_filter in tables:
        # Create MonitoringService with UNITUP and BLTH filters
        monitoring = MonitoringService(get_db_connection2, up_filter, selected_blth)
        
        # Rekap data
        all_recap_data.append(monitoring.fetch_status_recap(tbl, filter_marking=False))
        all_marking_data.append(monitoring.fetch_status_recap(tbl, filter_marking=True))
        
        # KET summary
        all_ket_data.append(monitoring.fetch_ket_summary(tbl, filter_marking=False))
        all_ket_marking_data.append(monitoring.fetch_ket_summary(tbl, filter_marking=True))
        
        # Per-table recap
        per_table_recap[f"{name}_{up_filter}"] = monitoring.fetch_status_recap(tbl, filter_marking=False)
        per_table_marking[f"{name}_{up_filter}"] = monitoring.fetch_status_recap(tbl, filter_marking=True)
        
        # Pivot tables
        pivot_key = f"{name}_{up_filter}"
        pivot_tables_all[pivot_key] = monitoring.generate_pivot_status(tbl, filter_marking=False)
        pivot_tables_marking[pivot_key] = monitoring.generate_pivot_status(tbl, filter_marking=True)
        pivot_tables_dlpd[pivot_key] = monitoring.generate_pivot_dlpd_hitung(tbl, filter_marking=True)
        pivot_tables_ganda[pivot_key] = monitoring.generate_pivot_ganda(tbl)
        pivot_tables_koreksi[pivot_key] = monitoring.generate_pivot_status_koreksi(tbl, filter_marking=True)
    
    # Combine all data for overall summary
    monitoring_combined = MonitoringService(get_db_connection2, None, selected_blth)
    recap_all, total_all = monitoring_combined.combine_recap_data(all_recap_data)
    recap_marking, total_marking = monitoring_combined.combine_recap_data(all_marking_data)
    summary_all, _ = monitoring_combined.summarize_ket_grouped(all_ket_data)
    summary_marking, _ = monitoring_combined.summarize_ket_grouped(all_ket_marking_data)
    
    return render_template("monitoring_dashboard.html",
        tables=tables,
        recap_all=recap_all, 
        total_all=total_all, 
        summary_all=summary_all,
        recap_marking=recap_marking, 
        total_marking=total_marking, 
        summary_marking=summary_marking,
        per_table_recap=per_table_recap,
        per_table_marking=per_table_marking,
        pivot_tables_all=pivot_tables_all,
        pivot_tables_marking=pivot_tables_marking,
        pivot_tables_dlpd=pivot_tables_dlpd,
        pivot_tables_koreksi=pivot_tables_koreksi,
        pivot_tables_ganda=pivot_tables_ganda,
        available_unitup=available_unitup,
        available_blth=available_blth,
        selected_unitup=selected_unitup,
        selected_blth=selected_blth,
        username=user
    )

# ==================== DETAIL POPUP ROUTES ====================

@monitoring_bp.route('/get_detail_pelanggan')
@login_required
def get_detail_pelanggan():
    """Get customer details for popup"""
    from .monitoring_service import MonitoringService
    
    username = session.get('username', '')
    unitup_filter = get_user_unitup_filter(username)
    monitoring = MonitoringService(get_db_connection2, unitup_filter)
    
    blth = request.args.get('blth')
    kdkelompok = request.args.get('kdkelompok')
    hasil_pemeriksaan = request.args.get('hasil_pemeriksaan')
    dlpd_hitung = request.args.get('dlpd_hitung')
    table = request.args.get('table')
    username = session.get('username', '')
    
    # Validation
    if not blth or not kdkelompok or not table:
        return jsonify({'error': 'Parameter wajib tidak lengkap'}), 400
    
    if kdkelompok not in [str(i) for i in range(1, 9)] + ['P']:
        return jsonify({'error': 'Invalid KDKELOMPOK value'}), 400
    
    # Check table access
    filter_marking = table.endswith('_marking')
    base_table = table[:-8] if filter_marking else table
    
    if not validate_table_access(username, base_table):
        return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
    
    # Get data
    result = monitoring.get_detail_pelanggan(
        base_table, blth, kdkelompok, 
        hasil_pemeriksaan, dlpd_hitung, filter_marking
    )
    
    return jsonify(result)


@monitoring_bp.route('/get_detail_pelanggan_koreksi')
@login_required
def get_detail_pelanggan_koreksi():
    """Get correction customer details"""
    from .monitoring_service import MonitoringService
    
    username = session.get('username', '')
    selected_unitup = request.args.get('unitup')
    selected_blth = request.args.get('blth')
    
    # Get UNITUP filter
    unitup_filter = get_user_unitup_filter(username, selected_unitup)
    
    # Create service with filters
    monitoring = MonitoringService(get_db_connection2, unitup_filter, selected_blth)
    
    blth = request.args.get('blth')
    kdkelompok = request.args.get('kdkelompok')
    hasil_pemeriksaan = request.args.get('hasil_pemeriksaan')
    table = request.args.get('table')
    
    # Validation
    if not blth or not kdkelompok or not table:
        return jsonify({'error': 'Parameter wajib tidak lengkap'}), 400
    
    if kdkelompok not in [str(i) for i in range(1, 9)] + ['P']:
        return jsonify({'error': 'Invalid KDKELOMPOK value'}), 400
    
    base_table = table.replace('_marking', '')
    if not validate_table_access(username, base_table):
        return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
    
    result = monitoring.get_detail_koreksi(base_table, blth, kdkelompok, hasil_pemeriksaan)
    return jsonify(result)

# ==================== SEARCH & UPDATE ROUTES ====================

@monitoring_bp.route('/search_idpel')
@login_required
def search_idpel():
    """Search customer by IDPEL"""
    from .monitoring_service import MonitoringService
    monitoring = MonitoringService(get_db_connection2)
    
    try:
        idpel = request.args.get('idpel')
        table = request.args.get('table')
        username = session.get('username', '')
        
        if not idpel:
            return jsonify({'error': 'IDPEL parameter is required'}), 400
        
        base_table = table.replace('_marking', '')
        if not validate_table_access(username, base_table):
            return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
        
        data = monitoring.search_customer_by_idpel(table, idpel)
        
        if not data:
            return jsonify({'error': 'Customer not found'}), 404
        
        return jsonify({'data': data})
    
    except Exception as e:
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500

@monitoring_bp.route('/get_full_customer_detail')
@login_required
def get_full_customer_detail():
    """Get full customer details by IDPEL"""
    from .monitoring_service import MonitoringService
    monitoring = MonitoringService(get_db_connection2)
    
    try:
        idpel = request.args.get('idpel')
        table = request.args.get('table')
        username = session.get('username', '')
        
        if not idpel:
            return jsonify({'error': 'IDPEL parameter is required'}), 400
        
        base_table = table.replace('_marking', '')
        if not validate_table_access(username, base_table):
            return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
        
        data = monitoring.search_customer_by_idpel(table, idpel)
        
        if not data:
            return jsonify({'error': 'Customer not found'}), 404
        
        return jsonify({'data': data})
    
    except Exception as e:
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500
    
# ==================== UPDATE ROUTE ====================

@monitoring_bp.route('/update_hasil_pemeriksaan', methods=['POST'])
@login_required
def update_hasil_pemeriksaan():
    """Update inspection results"""
    from .monitoring_service import MonitoringService
    
    try:
        # Get JSON data
        data = request.get_json()
        
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
        
        table = data.get('table')
        updates = data.get('updates', [])
        username = session.get('username', '')
        
        print(f"[UPDATE] User: {username}")
        print(f"[UPDATE] Table: {table}")
        print(f"[UPDATE] Updates count: {len(updates)}")
        
        if not table:
            return jsonify({"status": "error", "message": "Table parameter required"}), 400
        
        if not updates:
            return jsonify({"status": "error", "message": "No updates provided"}), 400
        
        # Remove _marking suffix if present
        base_table = table.replace('_marking', '')
        
        # Validate table access
        if not validate_table_access(username, base_table):
            return jsonify({"status": "error", "message": "Akses ditolak"}), 403
        
        # Get filters from request or session
        selected_unitup = request.args.get('unitup') or request.json.get('unitup')
        selected_blth = request.args.get('blth') or request.json.get('blth')
        
        # Get UNITUP filter
        unitup_filter = get_user_unitup_filter(username, selected_unitup)
        
        print(f"[UPDATE] UNITUP filter: {unitup_filter}")
        print(f"[UPDATE] BLTH filter: {selected_blth}")
        
        # Create monitoring service with filters
        monitoring = MonitoringService(get_db_connection2, unitup_filter, selected_blth)
        
        # Perform update
        result = monitoring.update_hasil_pemeriksaan(base_table, updates)
        
        if result['status'] == 'error':
            print(f"[UPDATE ERROR] {result['message']}")
            return jsonify(result), 500
        
        print(f"[UPDATE SUCCESS] {result['message']}")
        return jsonify(result)
    
    except Exception as e:
        print(f"[UPDATE EXCEPTION] {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Internal error: {str(e)}"}), 500


# ==================== DEBUG ROUTE ====================

@monitoring_bp.route('/debug_user_access')
@login_required
def debug_user_access():
    """Debug route to check user access"""
    username = session.get('username', '')
    
    user_info = get_user_info(username)
    all_billing = get_all_billing_tables()
    user_tables = get_user_tables(username)
    
    debug_info = {
        'username': username,
        'user_info': user_info,
        'all_billing_tables': all_billing,
        'user_accessible_tables': user_tables,
    }
    
    if user_info:
        unitup = user_info.get('unitup', '')
        nama_ulp = user_info.get('nama_ulp', '')
        resolved_table = get_table_from_unitup(unitup, nama_ulp)
        
        debug_info['resolved_table'] = resolved_table
        debug_info['is_admin'] = (unitup == 'UP3' or 'Administrator' in str(nama_ulp))
    
    return jsonify(debug_info)


# ==================== ADDITIONAL ROUTES ====================

@monitoring_bp.route('/get_detail_pelanggan_dlpd')
@login_required
def get_detail_pelanggan_dlpd():
    """Get customer details filtered by DLPD_HITUNG"""
    from .monitoring_service import MonitoringService
    
    username = session.get('username', '')
    selected_unitup = request.args.get('unitup')
    selected_blth = request.args.get('blth')
    
    # Get UNITUP filter
    unitup_filter = get_user_unitup_filter(username, selected_unitup)
    
    # Create service with filters
    monitoring = MonitoringService(get_db_connection2, unitup_filter, selected_blth)
    
    dlpd_hitung = request.args.get('dlpd_hitung')
    hasil_pemeriksaan = request.args.get('hasil_pemeriksaan', '')
    table = request.args.get('table')
    
    # ✅ REVERSE MAPPING
    try:
        reverse_mapping = monitoring.get_dlpd_reverse_mapping()
        dlpd_for_db = reverse_mapping.get(dlpd_hitung, dlpd_hitung)
        print(f"[DEBUG DLPD] Frontend: {dlpd_hitung} -> DB: {dlpd_for_db}")
    except AttributeError:
        print("[WARNING] get_dlpd_reverse_mapping not found")
        dlpd_for_db = dlpd_hitung
    
    if not table or not dlpd_for_db:
        return jsonify({'error': 'Parameter table dan dlpd_hitung wajib ada'}), 400
    
    filter_marking = table.endswith('_marking')
    base_table = table[:-8] if filter_marking else table
    
    if not validate_table_access(username, base_table):
        return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
    
    print(f"[INFO] Fetching DLPD data: dlpd={dlpd_for_db}, hasil={hasil_pemeriksaan}")
    
    result = monitoring.get_detail_by_dlpd(
        base_table, dlpd_for_db, hasil_pemeriksaan, 
        filter_marking, ['1','2','3','4','5','6','7','8','P']
    )
    
    print(f"[INFO] Found {len(result.get('data', []))} records")
    
    return jsonify(result)


@monitoring_bp.route('/get_detail_pelanggan_ganda')
@login_required
def get_detail_pelanggan_ganda():
    """Get duplicate customer details (KDKELOMPOK A and I)"""
    from .monitoring_service import MonitoringService
    
    username = session.get('username', '')
    selected_unitup = request.args.get('unitup')
    selected_blth = request.args.get('blth')
    
    # Get UNITUP filter
    unitup_filter = get_user_unitup_filter(username, selected_unitup)
    
    # Create service with filters
    monitoring = MonitoringService(get_db_connection2, unitup_filter, selected_blth)
    
    dlpd_hitung = request.args.get('dlpd_hitung')
    hasil_pemeriksaan = request.args.get('hasil_pemeriksaan', '')
    table = request.args.get('table')
    
    # ✅ REVERSE MAPPING
    try:
        reverse_mapping = monitoring.get_dlpd_reverse_mapping()
        dlpd_for_db = reverse_mapping.get(dlpd_hitung, dlpd_hitung)
        print(f"[DEBUG GANDA] Frontend: {dlpd_hitung} -> DB: {dlpd_for_db}")
    except AttributeError:
        print("[WARNING] get_dlpd_reverse_mapping not found")
        dlpd_for_db = dlpd_hitung
    
    if not table or not dlpd_for_db:
        return jsonify({'error': 'Parameter table dan dlpd_hitung wajib ada'}), 400
    
    filter_marking = table.endswith('_marking')
    base_table = table[:-8] if filter_marking else table
    
    if not validate_table_access(username, base_table):
        return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
    
    print(f"[INFO] Fetching GANDA data: dlpd={dlpd_for_db}, hasil={hasil_pemeriksaan}")
    
    result = monitoring.get_detail_by_dlpd(
        base_table, dlpd_for_db, hasil_pemeriksaan, 
        filter_marking, ['A','I']
    )
    
    print(f"[INFO] Found {len(result.get('data', []))} records")
    
    return jsonify(result)


"""
Monitoring Routes Module
Flask routes for monitoring dashboard with dynamic table access
"""

from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from functools import wraps
import pymysql

# Create Blueprint
monitoring_bp = Blueprint('monitoring', __name__, url_prefix='/monitoring')

def login_required(f):
    """Decorator to require login"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated_function

def get_all_billing_tables():
    """Get all billing tables from database"""
    return ['billing']

def get_user_info(username):
    """Get user information from tb_user"""
    conn = get_db_connection2()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT unitup, nama_ulp 
            FROM tb_user 
            WHERE username = %s
        """, (username,))
        
        return cursor.fetchone()
    finally:
        cursor.close()
        conn.close()

def get_latest_blth():
    """Get the latest BLTH from billing table"""
    conn = get_db_connection2()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT MAX(BLTH) as latest_blth 
            FROM billing 
            WHERE BLTH IS NOT NULL
        """)
        result = cursor.fetchone()
        return result['latest_blth'] if result else None
    finally:
        cursor.close()
        conn.close()

def get_available_blth_list():
    """Get list of available BLTH values"""
    conn = get_db_connection2()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT DISTINCT BLTH 
            FROM billing 
            WHERE BLTH IS NOT NULL 
            ORDER BY BLTH DESC
        """)
        return [row['BLTH'] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

def get_available_unitup_list():
    """Get list of available UNITUP values"""
    conn = get_db_connection2()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT DISTINCT u.unitup, u.nama_ulp 
            FROM tb_user u
            INNER JOIN billing b ON u.unitup = b.UNITUP
            WHERE u.unitup IS NOT NULL 
            AND u.unitup != 'UP3'
            ORDER BY u.unitup
        """)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

def get_user_unitup_filter(username, override_unitup=None):
    """Get UNITUP filter for user"""
    user_info = get_user_info(username)
    
    if not user_info:
        return None
    
    unitup = user_info.get('unitup', '')
    nama_ulp = user_info.get('nama_ulp', '')
    
    # Jika UP3 atau Administrator
    if unitup == 'UP3' or 'Administrator' in str(nama_ulp) or 'UP3' in str(nama_ulp).upper():
        # Jika ada override dari filter, gunakan itu
        return override_unitup if override_unitup else None
    
    # Return unitup user untuk filter WHERE clause
    return unitup

def get_user_tables(username, selected_unitup=None):
    """Get accessible tables for user based on their unitup"""
    user_info = get_user_info(username)
    
    if not user_info:
        print(f"[DEBUG] User info not found for: {username}")
        return []
    
    unitup = user_info.get('unitup', '')
    nama_ulp = user_info.get('nama_ulp', '')
    
    print(f"[DEBUG] User: {username}, UnitUp: {unitup}, Nama ULP: {nama_ulp}")
    
    # Check if admin
    is_admin = unitup == 'UP3' or 'Administrator' in str(nama_ulp) or 'UP3' in str(nama_ulp).upper()
    
    if is_admin:
        # Admin: Get all ULPs or selected one
        if selected_unitup:
            conn = get_db_connection2()
            cursor = conn.cursor()
            cursor.execute("SELECT nama_ulp FROM tb_user WHERE unitup = %s LIMIT 1", (selected_unitup,))
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            ulp_name = result['nama_ulp'].upper() if result else f'UNITUP {selected_unitup}'
            return [('billing', ulp_name, selected_unitup)]
        else:
            # Get all ULPs
            conn = get_db_connection2()
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT UNITUP FROM billing WHERE UNITUP IS NOT NULL ORDER BY UNITUP")
            unitup_list = [row['UNITUP'] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            tables = []
            for up in unitup_list:
                conn = get_db_connection2()
                cursor = conn.cursor()
                cursor.execute("SELECT nama_ulp FROM tb_user WHERE unitup = %s LIMIT 1", (up,))
                result = cursor.fetchone()
                cursor.close()
                conn.close()
                
                ulp_name = result['nama_ulp'].upper() if result else f'UNITUP {up}'
                tables.append(('billing', ulp_name, up))
            
            return tables
    else:
        # Regular user: Only their ULP
        display_name = nama_ulp.upper() if nama_ulp else 'BILLING'
        return [('billing', display_name, unitup)]

def validate_table_access(username, table):
    """Validate if user has access to table"""
    user_info = get_user_info(username)
    
    if not user_info:
        return False
    
    unitup = user_info.get('unitup', '')
    nama_ulp = user_info.get('nama_ulp', '')
    
    # Administrator UP3 bisa akses semua
    if unitup == 'UP3' or 'Administrator' in str(nama_ulp) or 'UP3' in str(nama_ulp).upper():
        return True
    
    return True  # For single billing table, allow access

# ==================== MAIN DASHBOARD ROUTE ====================

@monitoring_bp.route("/dashboard")
@login_required
def dashboard_monitoring():
    """Main monitoring dashboard with UNITUP and BLTH filters"""
    from .monitoring_service import MonitoringService
    
    user = session.get('username', '')
    user_info = get_user_info(user)
    
    if not user_info:
        print(f"[WARNING] User {user} info not found")
        return render_template("monitoring_dashboard.html",
            tables=[],
            recap_all={}, 
            total_all={}, 
            summary_all={'NAIK': {'jumlah': 0, 'persentase': 0}, 
                        'TURUN': {'jumlah': 0, 'persentase': 0},
                        'DIV/NA': {'jumlah': 0, 'persentase': 0},
                        'AMAN': {'jumlah': 0, 'persentase': 0}},
            recap_marking={}, 
            total_marking={}, 
            summary_marking={'NAIK': {'jumlah': 0, 'persentase': 0}, 
                            'TURUN': {'jumlah': 0, 'persentase': 0},
                            'DIV/NA': {'jumlah': 0, 'persentase': 0},
                            'AMAN': {'jumlah': 0, 'persentase': 0}},
            per_table_recap={},
            per_table_marking={},
            pivot_tables_all={},
            pivot_tables_marking={},
            pivot_tables_dlpd={},
            pivot_tables_koreksi={},
            pivot_tables_ganda={},
            available_unitup=[],
            available_blth=[],
            selected_unitup=None,
            selected_blth=None,
            error_message="User info tidak ditemukan.",
            username=user
        )
    
    unitup = user_info.get('unitup', '')
    nama_ulp = user_info.get('nama_ulp', '')
    
    # Check if admin
    is_admin = unitup == 'UP3' or 'Administrator' in str(nama_ulp) or 'UP3' in str(nama_ulp).upper()
    
    # ✅ PERUBAHAN: Menggunakan logika redirect dari Kode 1
    # Get filter parameters from request
    selected_unitup = request.args.get('unitup')
    selected_blth = request.args.get('blth')  # None if not present

    # If blth param is missing entirely, redirect to same route with default blth
    if 'blth' not in request.args:
        default_blth = get_latest_blth()  # ambil BLTH terbaru
        # preserve unitup in URL (could be None or empty string)
        return redirect(url_for('.dashboard_monitoring', unitup=selected_unitup or '', blth=default_blth))
    
    # Get available options for filters
    available_blth = get_available_blth_list()
    available_unitup = get_available_unitup_list() if is_admin else []
    
    # Get list of ULPs to display
    tables = get_user_tables(user, selected_unitup)
    
    if not tables:
        print(f"[WARNING] No tables for user {user}")
        return render_template("monitoring_dashboard.html",
            tables=[],
            recap_all={}, 
            total_all={}, 
            summary_all={'NAIK': {'jumlah': 0, 'persentase': 0}, 
                        'TURUN': {'jumlah': 0, 'persentase': 0},
                        'DIV/NA': {'jumlah': 0, 'persentase': 0},
                        'AMAN': {'jumlah': 0, 'persentase': 0}},
            recap_marking={}, 
            total_marking={}, 
            summary_marking={'NAIK': {'jumlah': 0, 'persentase': 0}, 
                            'TURUN': {'jumlah': 0, 'persentase': 0},
                            'DIV/NA': {'jumlah': 0, 'persentase': 0},
                            'AMAN': {'jumlah': 0, 'persentase': 0}},
            per_table_recap={},
            per_table_marking={},
            pivot_tables_all={},
            pivot_tables_marking={},
            pivot_tables_dlpd={},
            pivot_tables_koreksi={},
            pivot_tables_ganda={},
            available_unitup=available_unitup,
            available_blth=available_blth,
            selected_unitup=selected_unitup,
            selected_blth=selected_blth,
            error_message="Tidak ada data ULP yang tersedia.",
            username=user
        )
    
    print(f"[INFO] User {user} accessing ULPs: {[t[1] for t in tables]}, BLTH: {selected_blth}")
    
    # Prepare data structures
    all_recap_data = []
    all_marking_data = []
    all_ket_data = []
    all_ket_marking_data = []
    
    per_table_recap = {}
    per_table_marking = {}
    pivot_tables_all = {}
    pivot_tables_marking = {}
    pivot_tables_dlpd = {}
    pivot_tables_ganda = {}
    pivot_tables_koreksi = {}
    
    # Generate data for each ULP with BLTH filter
    for tbl, name, up_filter in tables:
        # Create MonitoringService with UNITUP and BLTH filters
        monitoring = MonitoringService(get_db_connection2, up_filter, selected_blth)
        
        # Rekap data
        all_recap_data.append(monitoring.fetch_status_recap(tbl, filter_marking=False))
        all_marking_data.append(monitoring.fetch_status_recap(tbl, filter_marking=True))
        
        # KET summary
        all_ket_data.append(monitoring.fetch_ket_summary(tbl, filter_marking=False))
        all_ket_marking_data.append(monitoring.fetch_ket_summary(tbl, filter_marking=True))
        
        # Per-table recap
        per_table_recap[f"{name}_{up_filter}"] = monitoring.fetch_status_recap(tbl, filter_marking=False)
        per_table_marking[f"{name}_{up_filter}"] = monitoring.fetch_status_recap(tbl, filter_marking=True)
        
        # Pivot tables
        pivot_key = f"{name}_{up_filter}"
        pivot_tables_all[pivot_key] = monitoring.generate_pivot_status(tbl, filter_marking=False)
        pivot_tables_marking[pivot_key] = monitoring.generate_pivot_status(tbl, filter_marking=True)
        pivot_tables_dlpd[pivot_key] = monitoring.generate_pivot_dlpd_hitung(tbl, filter_marking=True)
        pivot_tables_ganda[pivot_key] = monitoring.generate_pivot_ganda(tbl)
        pivot_tables_koreksi[pivot_key] = monitoring.generate_pivot_status_koreksi(tbl, filter_marking=True)
    
    # Combine all data for overall summary
    monitoring_combined = MonitoringService(get_db_connection2, None, selected_blth)
    recap_all, total_all = monitoring_combined.combine_recap_data(all_recap_data)
    recap_marking, total_marking = monitoring_combined.combine_recap_data(all_marking_data)
    summary_all, _ = monitoring_combined.summarize_ket_grouped(all_ket_data)
    summary_marking, _ = monitoring_combined.summarize_ket_grouped(all_ket_marking_data)
    
    return render_template("monitoring_dashboard.html",
        tables=tables,
        recap_all=recap_all, 
        total_all=total_all, 
        summary_all=summary_all,
        recap_marking=recap_marking, 
        total_marking=total_marking, 
        summary_marking=summary_marking,
        per_table_recap=per_table_recap,
        per_table_marking=per_table_marking,
        pivot_tables_all=pivot_tables_all,
        pivot_tables_marking=pivot_tables_marking,
        pivot_tables_dlpd=pivot_tables_dlpd,
        pivot_tables_koreksi=pivot_tables_koreksi,
        pivot_tables_ganda=pivot_tables_ganda,
        available_unitup=available_unitup,
        available_blth=available_blth,
        selected_unitup=selected_unitup,
        selected_blth=selected_blth,
        username=user
    )

# ==================== DETAIL POPUP ROUTES ====================

@monitoring_bp.route('/get_detail_pelanggan')
@login_required
def get_detail_pelanggan():
    """Get customer details for popup"""
    from .monitoring_service import MonitoringService
    
    username = session.get('username', '')
    unitup_filter = get_user_unitup_filter(username)
    monitoring = MonitoringService(get_db_connection2, unitup_filter)
    
    blth = request.args.get('blth')
    kdkelompok = request.args.get('kdkelompok')
    hasil_pemeriksaan = request.args.get('hasil_pemeriksaan')
    dlpd_hitung = request.args.get('dlpd_hitung')
    table = request.args.get('table')
    username = session.get('username', '')
    
    # Validation
    if not blth or not kdkelompok or not table:
        return jsonify({'error': 'Parameter wajib tidak lengkap'}), 400
    
    if kdkelompok not in [str(i) for i in range(1, 9)] + ['P']:
        return jsonify({'error': 'Invalid KDKELOMPOK value'}), 400
    
    # Check table access
    filter_marking = table.endswith('_marking')
    base_table = table[:-8] if filter_marking else table
    
    if not validate_table_access(username, base_table):
        return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
    
    # Get data
    result = monitoring.get_detail_pelanggan(
        base_table, blth, kdkelompok, 
        hasil_pemeriksaan, dlpd_hitung, filter_marking
    )
    
    return jsonify(result)


@monitoring_bp.route('/get_detail_pelanggan_koreksi')
@login_required
def get_detail_pelanggan_koreksi():
    """Get correction customer details"""
    from .monitoring_service import MonitoringService
    
    username = session.get('username', '')
    selected_unitup = request.args.get('unitup')
    selected_blth = request.args.get('blth')
    
    # Get UNITUP filter
    unitup_filter = get_user_unitup_filter(username, selected_unitup)
    
    # Create service with filters
    monitoring = MonitoringService(get_db_connection2, unitup_filter, selected_blth)
    
    blth = request.args.get('blth')
    kdkelompok = request.args.get('kdkelompok')
    hasil_pemeriksaan = request.args.get('hasil_pemeriksaan')
    table = request.args.get('table')
    
    # Validation
    if not blth or not kdkelompok or not table:
        return jsonify({'error': 'Parameter wajib tidak lengkap'}), 400
    
    if kdkelompok not in [str(i) for i in range(1, 9)] + ['P']:
        return jsonify({'error': 'Invalid KDKELOMPOK value'}), 400
    
    base_table = table.replace('_marking', '')
    if not validate_table_access(username, base_table):
        return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
    
    result = monitoring.get_detail_koreksi(base_table, blth, kdkelompok, hasil_pemeriksaan)
    return jsonify(result)

# ==================== SEARCH & UPDATE ROUTES ====================

@monitoring_bp.route('/search_idpel')
@login_required
def search_idpel():
    """Search customer by IDPEL"""
    from .monitoring_service import MonitoringService
    monitoring = MonitoringService(get_db_connection2)
    
    try:
        idpel = request.args.get('idpel')
        table = request.args.get('table')
        username = session.get('username', '')
        
        if not idpel:
            return jsonify({'error': 'IDPEL parameter is required'}), 400
        
        base_table = table.replace('_marking', '')
        if not validate_table_access(username, base_table):
            return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
        
        data = monitoring.search_customer_by_idpel(table, idpel)
        
        if not data:
            return jsonify({'error': 'Customer not found'}), 404
        
        return jsonify({'data': data})
    
    except Exception as e:
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500

@monitoring_bp.route('/get_full_customer_detail')
@login_required
def get_full_customer_detail():
    """Get full customer details by IDPEL"""
    from .monitoring_service import MonitoringService
    monitoring = MonitoringService(get_db_connection2)
    
    try:
        idpel = request.args.get('idpel')
        table = request.args.get('table')
        username = session.get('username', '')
        
        if not idpel:
            return jsonify({'error': 'IDPEL parameter is required'}), 400
        
        base_table = table.replace('_marking', '')
        if not validate_table_access(username, base_table):
            return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
        
        data = monitoring.search_customer_by_idpel(table, idpel)
        
        if not data:
            return jsonify({'error': 'Customer not found'}), 404
        
        return jsonify({'data': data})
    
    except Exception as e:
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500
    
# ==================== UPDATE ROUTE ====================

@monitoring_bp.route('/update_hasil_pemeriksaan', methods=['POST'])
@login_required
def update_hasil_pemeriksaan():
    """Update inspection results"""
    from .monitoring_service import MonitoringService
    
    try:
        # Get JSON data
        data = request.get_json()
        
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
        
        table = data.get('table')
        updates = data.get('updates', [])
        username = session.get('username', '')
        
        print(f"[UPDATE] User: {username}")
        print(f"[UPDATE] Table: {table}")
        print(f"[UPDATE] Updates count: {len(updates)}")
        
        if not table:
            return jsonify({"status": "error", "message": "Table parameter required"}), 400
        
        if not updates:
            return jsonify({"status": "error", "message": "No updates provided"}), 400
        
        # Remove _marking suffix if present
        base_table = table.replace('_marking', '')
        
        # Validate table access
        if not validate_table_access(username, base_table):
            return jsonify({"status": "error", "message": "Akses ditolak"}), 403
        
        # Get filters from request or session
        selected_unitup = request.args.get('unitup') or request.json.get('unitup')
        selected_blth = request.args.get('blth') or request.json.get('blth')
        
        # Get UNITUP filter
        unitup_filter = get_user_unitup_filter(username, selected_unitup)
        
        print(f"[UPDATE] UNITUP filter: {unitup_filter}")
        print(f"[UPDATE] BLTH filter: {selected_blth}")
        
        # Create monitoring service with filters
        monitoring = MonitoringService(get_db_connection2, unitup_filter, selected_blth)
        
        # Perform update
        result = monitoring.update_hasil_pemeriksaan(base_table, updates)
        
        if result['status'] == 'error':
            print(f"[UPDATE ERROR] {result['message']}")
            return jsonify(result), 500
        
        print(f"[UPDATE SUCCESS] {result['message']}")
        return jsonify(result)
    
    except Exception as e:
        print(f"[UPDATE EXCEPTION] {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Internal error: {str(e)}"}), 500


# ==================== DEBUG ROUTE ====================

@monitoring_bp.route('/debug_user_access')
@login_required
def debug_user_access():
    """Debug route to check user access"""
    username = session.get('username', '')
    
    user_info = get_user_info(username)
    all_billing = get_all_billing_tables()
    user_tables = get_user_tables(username)
    
    debug_info = {
        'username': username,
        'user_info': user_info,
        'all_billing_tables': all_billing,
        'user_accessible_tables': user_tables,
    }
    
    if user_info:
        unitup = user_info.get('unitup', '')
        nama_ulp = user_info.get('nama_ulp', '')
        resolved_table = get_table_from_unitup(unitup, nama_ulp)
        
        debug_info['resolved_table'] = resolved_table
        debug_info['is_admin'] = (unitup == 'UP3' or 'Administrator' in str(nama_ulp))
    
    return jsonify(debug_info)


# ==================== ADDITIONAL ROUTES ====================

@monitoring_bp.route('/get_detail_pelanggan_dlpd')
@login_required
def get_detail_pelanggan_dlpd():
    """Get customer details filtered by DLPD_HITUNG"""
    from .monitoring_service import MonitoringService
    
    username = session.get('username', '')
    selected_unitup = request.args.get('unitup')
    selected_blth = request.args.get('blth')
    
    # Get UNITUP filter
    unitup_filter = get_user_unitup_filter(username, selected_unitup)
    
    # Create service with filters
    monitoring = MonitoringService(get_db_connection2, unitup_filter, selected_blth)
    
    dlpd_hitung = request.args.get('dlpd_hitung')
    hasil_pemeriksaan = request.args.get('hasil_pemeriksaan', '')
    table = request.args.get('table')
    
    # ✅ REVERSE MAPPING
    try:
        reverse_mapping = monitoring.get_dlpd_reverse_mapping()
        dlpd_for_db = reverse_mapping.get(dlpd_hitung, dlpd_hitung)
        print(f"[DEBUG DLPD] Frontend: {dlpd_hitung} -> DB: {dlpd_for_db}")
    except AttributeError:
        print("[WARNING] get_dlpd_reverse_mapping not found")
        dlpd_for_db = dlpd_hitung
    
    if not table or not dlpd_for_db:
        return jsonify({'error': 'Parameter table dan dlpd_hitung wajib ada'}), 400
    
    filter_marking = table.endswith('_marking')
    base_table = table[:-8] if filter_marking else table
    
    if not validate_table_access(username, base_table):
        return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
    
    print(f"[INFO] Fetching DLPD data: dlpd={dlpd_for_db}, hasil={hasil_pemeriksaan}")
    
    result = monitoring.get_detail_by_dlpd(
        base_table, dlpd_for_db, hasil_pemeriksaan, 
        filter_marking, ['1','2','3','4','5','6','7','8','P']
    )
    
    print(f"[INFO] Found {len(result.get('data', []))} records")
    
    return jsonify(result)


@monitoring_bp.route('/get_detail_pelanggan_ganda')
@login_required
def get_detail_pelanggan_ganda():
    """Get duplicate customer details (KDKELOMPOK A and I)"""
    from .monitoring_service import MonitoringService
    
    username = session.get('username', '')
    selected_unitup = request.args.get('unitup')
    selected_blth = request.args.get('blth')
    
    # Get UNITUP filter
    unitup_filter = get_user_unitup_filter(username, selected_unitup)
    
    # Create service with filters
    monitoring = MonitoringService(get_db_connection2, unitup_filter, selected_blth)
    
    dlpd_hitung = request.args.get('dlpd_hitung')
    hasil_pemeriksaan = request.args.get('hasil_pemeriksaan', '')
    table = request.args.get('table')
    
    # ✅ REVERSE MAPPING
    try:
        reverse_mapping = monitoring.get_dlpd_reverse_mapping()
        dlpd_for_db = reverse_mapping.get(dlpd_hitung, dlpd_hitung)
        print(f"[DEBUG GANDA] Frontend: {dlpd_hitung} -> DB: {dlpd_for_db}")
    except AttributeError:
        print("[WARNING] get_dlpd_reverse_mapping not found")
        dlpd_for_db = dlpd_hitung
    
    if not table or not dlpd_for_db:
        return jsonify({'error': 'Parameter table dan dlpd_hitung wajib ada'}), 400
    
    filter_marking = table.endswith('_marking')
    base_table = table[:-8] if filter_marking else table
    
    if not validate_table_access(username, base_table):
        return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
    
    print(f"[INFO] Fetching GANDA data: dlpd={dlpd_for_db}, hasil={hasil_pemeriksaan}")
    
    result = monitoring.get_detail_by_dlpd(
        base_table, dlpd_for_db, hasil_pemeriksaan, 
        filter_marking, ['A','I']
    )
    
    print(f"[INFO] Found {len(result.get('data', []))} records")
    
    return jsonify(result)


@monitoring_bp.route('/get_detail_pelanggan_dlpd_hb')
@login_required
def get_detail_pelanggan_dlpd_hb():
    """Get customer details by BLTH, KDKELOMPOK with DLPD filter (including AMAN status)"""
    from .monitoring_service import MonitoringService
    
    username = session.get('username', '')
    request_unitup = request.args.get('unitup')
    selected_blth = request.args.get('blth')
    blth = request.args.get('blth')
    kdkelompok = request.args.get('kdkelompok')
    hasil_pemeriksaan = request.args.get('hasil_pemeriksaan')
    dlpd_hitung = request.args.get('dlpd_hitung')
    table = request.args.get('table')
    
    # Get UNITUP filter
    unitup_filter = get_user_unitup_filter(username, request_unitup)
    
    # Create service with filters
    monitoring = MonitoringService(get_db_connection2, unitup_filter, selected_blth)
    
    # ✅ REVERSE MAPPING
    try:
        reverse_mapping = monitoring.get_dlpd_reverse_mapping()
        dlpd_for_db = reverse_mapping.get(dlpd_hitung, dlpd_hitung)
        print(f"[DEBUG HB] Frontend: {dlpd_hitung} -> DB: {dlpd_for_db}")
    except AttributeError:
        print("[WARNING] get_dlpd_reverse_mapping not found")
        dlpd_for_db = dlpd_hitung
    
    # Validation
    if not blth or not kdkelompok or not table:
        print("[ERROR] Parameter tidak lengkap")
        return jsonify({'error': 'Parameter wajib tidak lengkap'}), 400
    
    if kdkelompok not in [str(i) for i in range(1, 9)] + ['P']:
        print(f"[ERROR] Invalid KDKELOMPOK: {kdkelompok}")
        return jsonify({'error': 'Invalid KDKELOMPOK value'}), 400
    
    # Check table access
    filter_marking = table.endswith('_marking')
    base_table = table[:-8] if filter_marking else table
    
    if not validate_table_access(username, base_table):
        print(f"[ERROR] Access denied for user {username} to table {base_table}")
        return jsonify({'error': 'Akses ditolak ke tabel ini'}), 403
    
    print(f"[INFO] Fetching HB data: table={base_table}, blth={blth}, kdkelompok={kdkelompok}, dlpd={dlpd_for_db}, unitup={unitup_filter}")
    
    # Get data with AMAN status included
    result = monitoring.get_detail_pelanggan_dlpd_hb(
        base_table, blth, kdkelompok, 
        hasil_pemeriksaan, dlpd_for_db, filter_marking
    )
    
    print(f"[INFO] Found {len(result.get('data', []))} records")
    
    return jsonify(result)


# ==================== UTILITY FUNCTION ====================

def get_db_connection2():
    """
    Database connection function
    Returns a PyMySQL connection with DictCursor
    """
    from .monitoring_config import DB_CONFIG
    
    return pymysql.connect(
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        charset=DB_CONFIG['charset'],
        port=DB_CONFIG.get('port', 3306),
        cursorclass=pymysql.cursors.DictCursor
    )