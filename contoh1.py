from flask import Flask, render_template, request, redirect, url_for, session, flash
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta

app = Flask(__name__)
app.secret_key = 'your_super_secret_key_here'

# Database connection
engine = create_engine("mysql+pymysql://root:@localhost/billing_gabungan", 
                      pool_pre_ping=True, 
                      pool_recycle=3600)

# Helper functions
def normalize_idpel(idpel):
    """Normalize IDPEL to 12 digits"""
    return str(idpel).strip().zfill(12)

def normalize_blth(blth):
    """Normalize BLTH to YYYYMM format"""
    return str(blth).replace('-', '').replace('/', '')[:6]

def get_previous_blth(blth, months_back=1):
    """Get previous BLTH"""
    date = datetime.strptime(blth, '%Y%m')
    prev_date = date - relativedelta(months=months_back)
    return prev_date.strftime('%Y%m')

def process_dpm_upload(df_upload, blth_kini, unitup):
    """
    📥 Process uploaded DPM file and save to database
    """
    try:
        # Standardize columns
        df_upload.columns = [c.strip().upper() for c in df_upload.columns]
        
        # Required columns
        required_cols = ['IDPEL', 'LWBPPAKAI']
        for col in required_cols:
            if col not in df_upload.columns:
                raise ValueError(f"Kolom {col} tidak ditemukan di file")
        
        # Add metadata
        df_upload['BLTH'] = blth_kini
        df_upload['UNITUP'] = unitup
        df_upload['IDPEL'] = df_upload['IDPEL'].apply(normalize_idpel)
        
        # Numeric columns
        numeric_cols = ['DAYA', 'SLALWBP', 'LWBPCABUT', 'LWBPPASANG', 'SAHLWBP', 'LWBPPAKAI']
        for col in numeric_cols:
            if col in df_upload.columns:
                df_upload[col] = pd.to_numeric(df_upload[col], errors='coerce').fillna(0).astype(int)
        
        # Select only database columns
        db_cols = ['BLTH', 'UNITUP', 'IDPEL', 'NAMA', 'TARIF', 'DAYA',
                   'SLALWBP', 'LWBPCABUT', 'LWBPPASANG', 'SAHLWBP', 'LWBPPAKAI', 'DLPD']
        df_final = df_upload[[c for c in db_cols if c in df_upload.columns]]
        
        # Save to DPM table (dengan ON DUPLICATE KEY UPDATE via raw SQL)
        save_dpm_with_upsert(df_final, engine)
        
        return len(df_final), None
        
    except Exception as e:
        return 0, str(e)


def save_dpm_with_upsert(df, engine):
    """
    💾 Save DPM data dengan INSERT ... ON DUPLICATE KEY UPDATE
    Ini lebih aman daripada DELETE + INSERT
    """
    records = df.to_dict('records')
    
    with engine.begin() as conn:
        for record in records:
            # Build dynamic query
            cols = ', '.join(record.keys())
            placeholders = ', '.join([f":{k}" for k in record.keys()])
            updates = ', '.join([f"{k}=VALUES({k})" for k in record.keys() if k not in ['BLTH', 'IDPEL']])
            
            sql = text(f"""
                INSERT INTO dpm ({cols})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {updates}
            """)
            
            conn.execute(sql, record)

# Billing Processor - Gabungan Fitur Terbaik
def process_billing_advanced(blth_kini, unitup, engine):
    """
    🔄 Process billing dengan semua fitur dari Kode 1
    - 4 bulan historis
    - 8 jenis anomali detection
    - DLPD_3BLN classification
    - Auto-generate links
    """
    try:
        blth_lalu = get_previous_blth(blth_kini, 1)
        blth_lalulalu = get_previous_blth(blth_kini, 2)
        blth_lalu3 = get_previous_blth(blth_kini, 3)
        
        # Fetch data 4 bulan
        query = text("""
            SELECT * FROM dpm 
            WHERE unitup = :unitup 
            AND blth IN (:kini, :lalu, :lalulalu, :lalu3)
        """)
        
        df_all = pd.read_sql(query, engine, params={
            'unitup': unitup,
            'kini': blth_kini,
            'lalu': blth_lalu,
            'lalulalu': blth_lalulalu,
            'lalu3': blth_lalu3
        })
        
        if df_all.empty:
            return pd.DataFrame(), "Tidak ada data DPM untuk periode ini"
        
        # Split by month
        df_kini = df_all[df_all['BLTH'] == blth_kini].copy()
        df_lalu = df_all[df_all['BLTH'] == blth_lalu][['IDPEL', 'LWBPPAKAI']].rename(columns={'LWBPPAKAI': 'LWBPPAKAI_Y'})
        df_lalulalu = df_all[df_all['BLTH'] == blth_lalulalu][['IDPEL', 'LWBPPAKAI']].rename(columns={'LWBPPAKAI': 'LWBPPAKAI_X'})
        df_lalu3 = df_all[df_all['BLTH'] == blth_lalu3][['IDPEL', 'LWBPPAKAI']].rename(columns={'LWBPPAKAI': 'LWBPPAKAI_Z'})
        
        # Merge all
        df_merged = (df_kini
                     .merge(df_lalu, on='IDPEL', how='left')
                     .merge(df_lalulalu, on='IDPEL', how='left')
                     .merge(df_lalu3, on='IDPEL', how='left'))
        
        # Recalculate LWBPPAKAI if missing
        lwbp_kosong = df_merged['LWBPPAKAI'].isna()
        df_merged['LWBPPAKAI'] = np.where(
            lwbp_kosong,
            (df_merged['LWBPCABUT'].fillna(0)
             - df_merged['SLALWBP'].fillna(0)
             + df_merged['SAHLWBP'].fillna(0)
             - df_merged['LWBPPASANG'].fillna(0)),
            df_merged['LWBPPAKAI']
        )
        
        # Calculate metrics
        delta = df_merged['LWBPPAKAI'] - df_merged['LWBPPAKAI_Y'].fillna(0)
        rerata = df_merged[['LWBPPAKAI_Y', 'LWBPPAKAI_X', 'LWBPPAKAI_Z']].fillna(0).mean(axis=1)
        
        with np.errstate(divide='ignore', invalid='ignore'):
            percentage = (delta / df_merged['LWBPPAKAI_Y'].replace(0, np.nan)) * 100
            percentage = np.nan_to_num(percentage, nan=0)
        
        daya_kw = df_merged['DAYA'] / 1000
        jam_nyala = (df_merged['LWBPPAKAI'] / daya_kw).replace([np.inf, -np.inf], 0).fillna(0)
        
        # ===== ADVANCED ANOMALY DETECTION (dari Kode 1) =====
        stan_mundur = (
            (df_merged['SAHLWBP'] < df_merged['SLALWBP']) &
            (df_merged['LWBPCABUT'].fillna(0) == 0) &
            (df_merged['LWBPPASANG'].fillna(0) == 0)
        )
        
        cek_pecahan = (
            (df_merged['LWBPCABUT'].fillna(0) != 0) |
            (df_merged['LWBPPASANG'].fillna(0) != 0)
        )
        
        is_na = df_merged['LWBPPAKAI_Y'].isna() | (df_merged['LWBPPAKAI_Y'] == 0)
        
        conditions = [
            (jam_nyala >= 720),
            cek_pecahan,
            stan_mundur,
            (percentage > 50),
            (is_na & (jam_nyala > 40)),
            (percentage < -50),
            (df_merged['LWBPPAKAI'] == 0),
            (jam_nyala > 0) & (jam_nyala < 40)
        ]
        
        choices = [
            'JN_720up',
            'Cek_Pecahan',
            'Stan_Mundur',
            'Naik_50%Up',
            'Cek_DIV/NA',
            'Turun_50%Down',
            'kWh_Nol',
            'JN_40Down'
        ]
        
        df_merged['DLPD_HITUNG'] = np.select(conditions, choices, default='')
        
        # DLPD_3BLN classification
        df_merged['DLPD_3BLN'] = np.where(
            (df_merged['LWBPPAKAI'].fillna(0) > 1.5 * rerata.fillna(0)),
            'Naik50% R3BLN',
            'Turun=50% R3BLN'
        )
        
        # KET classification
        is_naik = (~is_na) & (percentage >= 40)
        is_turun = (~is_na) & (percentage <= -40)
        ket = np.select(
            [is_na, is_naik, is_turun],
            ['DIV/NA', 'NAIK', 'TURUN'],
            default='AMAN'
        )
        
        # ===== BUILD FINAL DATAFRAME =====
        kroscek = pd.DataFrame({
            'BLTH': blth_kini,
            'UNITUP': unitup,
            'IDPEL': df_merged['IDPEL'],
            'NAMA': df_merged['NAMA'],
            'TARIF': df_merged['TARIF'],
            'DAYA': df_merged['DAYA'].fillna(0).astype(int),
            'KDKELOMPOK': df_merged.get('KDKELOMPOK', ''),
            'SLALWBP': df_merged['SLALWBP'].fillna(0).astype(int),
            'LWBPCABUT': df_merged['LWBPCABUT'].fillna(0).astype(int),
            'LWBPPASANG': df_merged['LWBPPASANG'].fillna(0).astype(int),
            'SAHLWBP': df_merged['SAHLWBP'].fillna(0).astype(int),
            'LWBPPAKAI': df_merged['LWBPPAKAI'].fillna(0).astype(int),
            'DELTA_PEMKWH': delta.fillna(0).astype(int),
            'PERSEN': pd.Series(percentage).round(1).astype(str) + '%',
            'KET': ket,
            'JAM_NYALA': jam_nyala.round(1),
            'JAMNYALA600': np.where(jam_nyala > 600, '600Up', '600Down'),
            'DLPD': df_merged.get('DLPD', ''),
            'DLPD_3BLN': df_merged['DLPD_3BLN'],
            'DLPD_HITUNG': df_merged['DLPD_HITUNG'],
            'NOMET': '',  # Will be filled by trigger from DIL
            'HASIL_PEMERIKSAAN': '',
            'TINDAK_LANJUT': '',
            'KETERANGAN': '',
            'MARKING_KOREKSI': 0
        })
        
        # ===== GENERATE LINKS (dari Kode 1) =====
        path_foto = 'https://portalapp.iconpln.co.id/acmt/DisplayBlobServlet1?idpel='
        
        kroscek['FOTO_AKHIR'] = kroscek['IDPEL'].apply(
            lambda x: f'<a href="{path_foto}{x}&blth={blth_kini}" target="_blank">FOTO</a>'
        )
        kroscek['FOTO_LALU'] = kroscek['IDPEL'].apply(
            lambda x: f'<a href="{path_foto}{x}&blth={blth_lalu}" target="_blank">FOTO</a>'
        )
        kroscek['FOTO_LALU2'] = kroscek['IDPEL'].apply(
            lambda x: f'<a href="{path_foto}{x}&blth={blth_lalulalu}" target="_blank">FOTO</a>'
        )
        kroscek['FOTO_3BLN'] = kroscek['IDPEL'].apply(
            lambda x: f'<a href="#" onclick="open3Foto(\'{x}\', \'{blth_kini}\'); return false;">{str(x)[-5:]}</a>'
        )
        
        # Grafik link (sesuaikan domain Anda)
        kroscek['GRAFIK'] = kroscek['IDPEL'].apply(
            lambda x: f'<a href="/grafik/{x}?blth={blth_kini}&ulp={unitup}" target="_blank">GRAFIK</a>'
        )
        
        return kroscek, None
        
    except Exception as e:
        return pd.DataFrame(), str(e)
  
  
    
#Save to Billing dengan Trigger
def save_to_billing_with_trigger(df, engine, username):
    """
    💾 Save ke billing table - trigger akan handle:
    - Auto-update MARKING_KOREKSI
    - Auto-log ke AUDIT_LOG
    - Auto-sync NOMET dari DIL
    """
    try:
        records = df.to_dict('records')
        success = 0
        failed = 0
        
        with engine.begin() as conn:
            for record in records:
                try:
                    # Tambahkan updated_by untuk trigger
                    record['updated_by'] = username
                    
                    # Build dynamic upsert query
                    cols = list(record.keys())
                    placeholders = ', '.join([f":{k}" for k in cols])
                    updates = ', '.join([
                        f"{k}=VALUES({k})" 
                        for k in cols 
                        if k not in ['BLTH', 'IDPEL']  # UNIQUE KEY columns
                    ])
                    
                    sql = text(f"""
                        INSERT INTO billing ({', '.join(cols)})
                        VALUES ({placeholders})
                        ON DUPLICATE KEY UPDATE {updates}
                    """)
                    
                    conn.execute(sql, record)
                    success += 1
                    
                except IntegrityError as e:
                    failed += 1
                    print(f"❌ Gagal simpan {record.get('IDPEL')}: {e}")
                    continue
        
        return {
            'success': success,
            'failed': failed,
            'message': f"Berhasil: {success}, Gagal: {failed}"
        }
        
    except Exception as e:
        return {'success': 0, 'failed': len(df), 'message': str(e)}

#update data
@app.route('/update_hasil_pemeriksaan', methods=['POST'])
def update_hasil_pemeriksaan():
    """
    💾 Batch update hasil pemeriksaan (dipanggil dari view_billing.html)
    """
    if 'loggedin' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    data = request.get_json()
    updates = data.get('updates', [])
    username = session.get('username')
    
    if not updates:
        return jsonify({'error': 'Tidak ada data untuk diupdate'}), 400
    
    try:
        success_count = 0
        failed_count = 0
        
        with engine.begin() as conn:
            for item in updates:
                try:
                    sql = text("""
                        UPDATE billing 
                        SET hasil_pemeriksaan = :hasil,
                            tindak_lanjut = :tindak_lanjut,
                            updated_by = :username
                        WHERE idpel = :idpel AND blth = :blth
                    """)
                    
                    result = conn.execute(sql, {
                        'hasil': item.get('hasil_pemeriksaan'),
                        'tindak_lanjut': item.get('tindak_lanjut'),
                        'username': username,
                        'idpel': normalize_idpel(item.get('idpel')),
                        'blth': item.get('blth')
                    })
                    
                    if result.rowcount > 0:
                        success_count += 1
                    else:
                        failed_count += 1
                        
                except Exception as e:
                    print(f"Failed to update {item.get('idpel')}: {e}")
                    failed_count += 1
                    continue
        
        return jsonify({
            'success': True,
            'message': f'Berhasil: {success_count}, Gagal: {failed_count}',
            'updated': success_count,
            'failed': failed_count
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
   
    
#v2

@app.route('/dashboard_ulp', methods=['GET', 'POST'])
def dashboard_ulp():
    """
    📊 Dashboard ULP dengan fitur lengkap dari Kode 1
    """
    if 'loggedin' not in session:
        return redirect(url_for('login'))
    
    unitup = session.get('unitup')
    username = session.get('username')
    nama = session.get('nama_ulp')
    
    if not unitup:
        flash('UNITUP tidak ditemukan di session', 'danger')
        return redirect(url_for('login'))
    
    # Default periode
    blth_kini = request.form.get('blth', datetime.now().strftime('%Y%m'))
    blth_kini = normalize_blth(blth_kini)
    
    # ===== UPLOAD FILE DPM =====
    if request.method == 'POST' and 'file_dpm' in request.files:
        file = request.files['file_dpm']
        
        if file.filename == '':
            flash('Tidak ada file yang dipilih', 'warning')
            return redirect(url_for('dashboard_ulp'))
        
        try:
            df_upload = pd.read_excel(file)
            count, error = process_dpm_upload(df_upload, blth_kini, unitup)
            
            if error:
                flash(f'Gagal upload: {error}', 'danger')
            else:
                flash(f'Berhasil upload {count} data DPM untuk {unitup}', 'success')
                
        except Exception as e:
            flash(f'Error processing file: {str(e)}', 'danger')
            return redirect(url_for('dashboard_ulp'))
    
    # ===== PROSES BILLING =====
    if request.method == 'POST' and request.form.get('action') == 'process_billing':
        df_billing, error = process_billing_advanced(blth_kini, unitup, engine)
        
        if error:
            flash(f'Gagal proses billing: {error}', 'danger')
        elif df_billing.empty:
            flash('Tidak ada data untuk diproses', 'warning')
        else:
            # Save to billing table (trigger akan handle sisanya)
            result = save_to_billing_with_trigger(df_billing, engine, username)
            flash(result['message'], 'success' if result['success'] > 0 else 'danger')
    
    # ===== AMBIL DATA UNTUK TAMPILAN =====
    try:
        query = text("""
            SELECT 
                blth,
                COUNT(*) as total,
                SUM(CASE WHEN ket = 'NAIK' THEN 1 ELSE 0 END) as naik,
                SUM(CASE WHEN ket = 'TURUN' THEN 1 ELSE 0 END) as turun,
                SUM(CASE WHEN ket = 'DIV/NA' THEN 1 ELSE 0 END) as div_na,
                SUM(CASE WHEN ket = 'AMAN' THEN 1 ELSE 0 END) as aman
            FROM billing
            WHERE unitup = :unitup
            GROUP BY blth
            ORDER BY blth DESC
            LIMIT 6
        """)
        
        df_summary = pd.read_sql(query, engine, params={'unitup': unitup})
        
    except Exception as e:
        flash(f'Gagal membaca data: {str(e)}', 'danger')
        df_summary = pd.DataFrame()
    
    return render_template(
        'dashboard_ulp.html',
        nama=nama,
        unitup=unitup,
        summary=df_summary.to_dict('records') if not df_summary.empty else [],
        blth_terakhir=blth_kini
    )


# =================== VIEW DATA BILLING ===================
@app.route('/view_billing', methods=['GET'])
def view_billing():
    """
    👁️ View data billing dengan filter dan editable form
    """
    if 'loggedin' not in session:
        return redirect(url_for('login'))
    
    unitup = session.get('unitup')
    username = session.get('username')
    role = session.get('role', 'ULP')
    
    # Filter parameters
    selected_blth = request.args.get('blth', '')
    selected_ket = request.args.get('ket', 'ALL')
    selected_kdkelompok = request.args.get('kdkelompok', '')
    jam_nyala_min = request.args.get('jam_nyala_min', type=float, default=0)
    jam_nyala_max = request.args.get('jam_nyala_max', type=float, default=9999)
    
    # Build query
    base_query = "SELECT * FROM billing WHERE 1=1"
    params = {}
    
    # Filter by UNITUP (kecuali role UP3)
    if role == 'ULP':
        base_query += " AND unitup = :unitup"
        params['unitup'] = unitup
    else:
        # UP3 bisa filter manual
        if request.args.get('unitup_filter'):
            base_query += " AND unitup = :unitup"
            params['unitup'] = request.args.get('unitup_filter')
    
    # Filter by BLTH
    if selected_blth:
        base_query += " AND blth = :blth"
        params['blth'] = selected_blth
    
    # Filter by KET
    if selected_ket != 'ALL':
        base_query += " AND ket = :ket"
        params['ket'] = selected_ket
    
    # Filter by KDKELOMPOK
    if selected_kdkelompok:
        base_query += " AND kdkelompok = :kdkelompok"
        params['kdkelompok'] = selected_kdkelompok
    
    # Filter by JAM NYALA
    if jam_nyala_min or jam_nyala_max != 9999:
        base_query += " AND jam_nyala BETWEEN :min AND :max"
        params['min'] = jam_nyala_min
        params['max'] = jam_nyala_max
    
    base_query += " ORDER BY idpel ASC LIMIT 1000"
    
    try:
        df_data = pd.read_sql(text(base_query), engine, params=params)
        
        # Get dropdown options
        blth_list = pd.read_sql(
            text("SELECT DISTINCT blth FROM billing ORDER BY blth DESC"),
            engine
        )['blth'].tolist()
        
        kdkelompok_list = pd.read_sql(
            text("SELECT DISTINCT kdkelompok FROM billing WHERE kdkelompok IS NOT NULL ORDER BY kdkelompok"),
            engine
        )['kdkelompok'].tolist()
        
    except Exception as e:
        flash(f'Gagal membaca data: {str(e)}', 'danger')
        df_data = pd.DataFrame()
        blth_list = []
        kdkelompok_list = []
    
    return render_template(
        'view_billing.html',
        data=df_data.to_dict('records') if not df_data.empty else [],
        blth_list=blth_list,
        kdkelompok_list=kdkelompok_list,
        selected_blth=selected_blth,
        selected_ket=selected_ket,
        selected_kdkelompok=selected_kdkelompok,
        jam_nyala_min=jam_nyala_min,
        jam_nyala_max=jam_nyala_max
    )





# =================== DASHBOARD UP3 ===================
@app.route('/dashboard_up3', methods=['GET'])
def dashboard_up3():
    """
    📊 Dashboard UP3 - Aggregasi semua ULP
    """
    if 'loggedin' not in session:
        return redirect(url_for('login'))
    
    if session.get('role') != 'UP3':
        flash('Akses ditolak. Hanya untuk user UP3.', 'danger')
        return redirect(url_for('dashboard_ulp'))
    
    # Filter parameters
    selected_unitup = request.args.get('unitup', 'ALL')
    selected_blth = request.args.get('blth', '')
    
    # Build query
    query = """
        SELECT 
            unitup,
            blth,
            COUNT(*) as total,
            SUM(CASE WHEN ket = 'NAIK' THEN 1 ELSE 0 END) as naik,
            SUM(CASE WHEN ket = 'TURUN' THEN 1 ELSE 0 END) as turun,
            SUM(CASE WHEN ket = 'DIV/NA' THEN 1 ELSE 0 END) as div_na,
            SUM(CASE WHEN ket = 'AMAN' THEN 1 ELSE 0 END) as aman,
            AVG(jam_nyala) as avg_jam_nyala
        FROM billing
        WHERE 1=1
    """
    params = {}
    
    if selected_unitup != 'ALL':
        query += " AND unitup = :unitup"
        params['unitup'] = selected_unitup
    
    if selected_blth:
        query += " AND blth = :blth"
        params['blth'] = selected_blth
    
    query += " GROUP BY unitup, blth ORDER BY blth DESC, unitup ASC"
    
    try:
        df_summary = pd.read_sql(text(query), engine, params=params)
        
        # Get ULP list
        unitup_list = pd.read_sql(
            text("SELECT DISTINCT unitup FROM billing ORDER BY unitup"),
            engine
        )['unitup'].tolist()
        
        # Get BLTH list
        blth_list = pd.read_sql(
            text("SELECT DISTINCT blth FROM billing ORDER BY blth DESC"),
            engine
        )['blth'].tolist()
        
    except Exception as e:
        flash(f'Gagal membaca data: {str(e)}', 'danger')
        df_summary = pd.DataFrame()
        unitup_list = []
        blth_list = []
    
    return render_template(
        'dashboard_up3.html',
        summary=df_summary.to_dict('records') if not df_summary.empty else [],
        unitup_list=unitup_list,
        blth_list=blth_list,
        selected_unitup=selected_unitup,
        selected_blth=selected_blth
    )


# =================== UPDATE NOMOR METER DARI DIL ===================
@app.route('/sync_nomet', methods=['POST'])
def sync_nomet():
    """
    🔄 Sync nomor meter dari tabel DIL ke billing
    Bisa manual trigger atau auto via trigger
    """
    if 'loggedin' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    unitup = session.get('unitup')
    role = session.get('role')
    
    # Filter by UNITUP (kecuali UP3)
    unitup_filter = ""
    params = {}
    
    if role == 'ULP':
        unitup_filter = "AND b.unitup = :unitup"
        params['unitup'] = unitup
    
    try:
        with engine.begin() as conn:
            # Update nomor meter dari DIL (ambil yang terbaru)
            sql = text(f"""
                UPDATE billing b
                JOIN (
                    SELECT d.idpel, d.nomet, d.created_at
                    FROM dil d
                    JOIN (
                        SELECT idpel, MAX(created_at) as max_date
                        FROM dil
                        WHERE nomet IS NOT NULL AND nomet != ''
                        GROUP BY idpel
                    ) latest ON d.idpel = latest.idpel AND d.created_at = latest.max_date
                ) d ON b.idpel = d.idpel
                SET b.nomet = d.nomet
                WHERE (b.nomet IS NULL OR b.nomet = '' OR b.nomet IN ('-', '0'))
                {unitup_filter}
            """)
            
            result = conn.execute(sql, params)
            updated_count = result.rowcount
            
        return jsonify({
            'success': True,
            'message': f'Berhasil sync {updated_count} nomor meter',
            'updated': updated_count
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =================== UPLOAD DIL (Data Induk Langganan) ===================
@app.route('/upload_dil', methods=['POST'])
def upload_dil():
    """
    📤 Upload data DIL (nomor meter, dll)
    """
    if 'loggedin' not in session:
        return redirect(url_for('login'))
    
    unitup = session.get('unitup')
    
    if 'file_dil' not in request.files:
        flash('Tidak ada file yang diupload', 'warning')
        return redirect(request.referrer or url_for('dashboard_ulp'))
    
    file = request.files['file_dil']
    
    if file.filename == '':
        flash('Tidak ada file yang dipilih', 'warning')
        return redirect(request.referrer or url_for('dashboard_ulp'))
    
    try:
        df = pd.read_excel(file)
        df.columns = [c.strip().upper() for c in df.columns]
        
        # Required columns
        required = ['IDPEL', 'NOMET']
        for col in required:
            if col not in df.columns:
                flash(f'Kolom {col} tidak ditemukan', 'danger')
                return redirect(request.referrer or url_for('dashboard_ulp'))
        
        # Normalize IDPEL
        df['IDPEL'] = df['IDPEL'].apply(normalize_idpel)
        df['UNITUP'] = unitup
        
        # Select columns
        upload_cols = ['IDPEL', 'UNITUP', 'NAMA', 'TARIF', 'DAYA', 'NOMET', 'ALAMAT']
        df_upload = df[[c for c in upload_cols if c in df.columns]]
        
        # Save to DIL table (append, no unique constraint)
        df_upload.to_sql('dil', engine, if_exists='append', index=False)
        
        flash(f'Berhasil upload {len(df_upload)} data DIL', 'success')
        
        # Auto-sync ke billing
        try:
            with engine.begin() as conn:
                sql = text("""
                    UPDATE billing b
                    JOIN dil d ON b.idpel = d.idpel
                    SET b.nomet = d.nomet
                    WHERE b.unitup = :unitup
                    AND (b.nomet IS NULL OR b.nomet = '')
                """)
                result = conn.execute(sql, {'unitup': unitup})
                flash(f'Auto-sync: {result.rowcount} nomor meter diupdate', 'info')
        except:
            pass
        
    except Exception as e:
        flash(f'Gagal upload DIL: {str(e)}', 'danger')
    
    return redirect(request.referrer or url_for('dashboard_ulp'))


# =================== VIEW AUDIT LOG ===================
@app.route('/audit_log', methods=['GET'])
def view_audit_log():
    """
    📜 View audit log perubahan data
    """
    if 'loggedin' not in session:
        return redirect(url_for('login'))
    
    # Filter
    idpel_filter = request.args.get('idpel', '')
    blth_filter = request.args.get('blth', '')
    days_back = request.args.get('days', type=int, default=7)
    
    query = """
        SELECT 
            id,
            table_name,
            idpel,
            blth,
            column_changed,
            old_value,
            new_value,
            changed_by,
            changed_at
        FROM audit_log
        WHERE changed_at >= DATE_SUB(NOW(), INTERVAL :days DAY)
    """
    params = {'days': days_back}
    
    if idpel_filter:
        query += " AND idpel = :idpel"
        params['idpel'] = normalize_idpel(idpel_filter)
    
    if blth_filter:
        query += " AND blth = :blth"
        params['blth'] = blth_filter
    
    query += " ORDER BY changed_at DESC LIMIT 500"
    
    try:
        df_log = pd.read_sql(text(query), engine, params=params)
    except Exception as e:
        flash(f'Gagal membaca audit log: {str(e)}', 'danger')
        df_log = pd.DataFrame()
    
    return render_template(
        'audit_log.html',
        logs=df_log.to_dict('records') if not df_log.empty else [],
        idpel_filter=idpel_filter,
        blth_filter=blth_filter,
        days_back=days_back
    )


# =================== EXPORT DATA ===================
@app.route('/export_billing', methods=['GET'])
def export_billing():
    """
    📥 Export data billing ke Excel
    """
    if 'loggedin' not in session:
        return redirect(url_for('login'))
    
    unitup = session.get('unitup')
    role = session.get('role')
    blth = request.args.get('blth', datetime.now().strftime('%Y%m'))
    
    query = "SELECT * FROM billing WHERE blth = :blth"
    params = {'blth': blth}
    
    if role == 'ULP':
        query += " AND unitup = :unitup"
        params['unitup'] = unitup
    
    try:
        df = pd.read_sql(text(query), engine, params=params)
        
        if df.empty:
            flash('Tidak ada data untuk diexport', 'warning')
            return redirect(request.referrer or url_for('dashboard_ulp'))
        
        # Remove HTML links
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(r'<[^>]*>', '', regex=True)
        
        # Create Excel file
        filename = f'billing_{unitup}_{blth}.xlsx'
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        
        df.to_excel(filepath, index=False, engine='openpyxl')
        
        return send_file(filepath, as_attachment=True, download_name=filename)
        
    except Exception as e:
        flash(f'Gagal export: {str(e)}', 'danger')
        return redirect(request.referrer or url_for('dashboard_ulp'))


# =================== GRAFIK PELANGGAN ===================
@app.route('/grafik/<idpel>', methods=['GET'])
def view_grafik(idpel):
    """
    📈 Tampilkan grafik pemakaian 6 bulan terakhir
    """
    idpel = normalize_idpel(idpel)
    blth = request.args.get('blth', datetime.now().strftime('%Y%m'))
    
    try:
        # Ambil 6 bulan terakhir
        query = text("""
            SELECT blth, lwbppakai, jam_nyala, delta_pemkwh
            FROM billing
            WHERE idpel = :idpel
            AND blth <= :blth
            ORDER BY blth DESC
            LIMIT 6
        """)
        
        df = pd.read_sql(query, engine, params={'idpel': idpel, 'blth': blth})
        
        if df.empty:
            return "Data tidak ditemukan", 404
        
        # Sort ascending untuk grafik
        df = df.sort_values('blth')
        
        return render_template(
            'grafik.html',
            idpel=idpel,
            labels=df['blth'].tolist(),
            lwbppakai=df['lwbppakai'].tolist(),
            jam_nyala=df['jam_nyala'].tolist(),
            delta=df['delta_pemkwh'].tolist()
        )
        
    except Exception as e:
        return f"Error: {str(e)}", 500


# =================== HELPER: CLEANUP OLD DATA ===================
def cleanup_old_dpm(months=6):
    """
    🧹 Hapus data DPM lebih dari X bulan
    Bisa dipanggil manual atau via scheduled job
    """
    try:
        cutoff_date = datetime.now() - relativedelta(months=months)
        cutoff_blth = cutoff_date.strftime('%Y%m')
        
        with engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM dpm WHERE CAST(blth AS UNSIGNED) < :cutoff"),
                {'cutoff': int(cutoff_blth)}
            )
            
        return result.rowcount
        
    except Exception as e:
        print(f"Error cleanup DPM: {e}")
        return 0


# =================== LOGIN ROUTE ===================
@app.route('/', methods=['GET', 'POST'])
def login():
    """
    🔐 Halaman login - Route utama
    """
    # Jika sudah login, redirect ke dashboard
    if 'loggedin' in session:
        if session.get('role') == 'UP3':
            return redirect(url_for('dashboard_up3'))
        else:
            return redirect(url_for('dashboard_ulp'))
    
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        
        if not username or not password:
            flash('Username dan password harus diisi', 'danger')
            return render_template('login.html')
        
        try:
            # Query user dari database
            query = text("""
                SELECT id_user, username, password, unitup, nama_ulp, role
                FROM tb_user
                WHERE username = :username
            """)
            
            with engine.connect() as conn:
                result = conn.execute(query, {'username': username}).fetchone()
            
            if result:
                # Verify password (SHA256)
                import hashlib
                hashed_input = hashlib.sha256(password.encode()).hexdigest()
                
                if hashed_input == result[2]:  # result[2] = password column
                    # Set session
                    session['loggedin'] = True
                    session['id_user'] = result[0]
                    session['username'] = result[1]
                    session['unitup'] = result[3]
                    session['nama_ulp'] = result[4]
                    session['role'] = result[5] or 'ULP'
                    
                    flash(f'Login berhasil! Selamat datang, {result[4]}', 'success')
                    
                    # Redirect berdasarkan role
                    if session['role'] == 'UP3':
                        return redirect(url_for('dashboard_up3'))
                    else:
                        return redirect(url_for('dashboard_ulp'))
                else:
                    flash('Password salah!', 'danger')
            else:
                flash('Username tidak ditemukan!', 'danger')
                
        except Exception as e:
            flash(f'Error login: {str(e)}', 'danger')
            print(f"Login error: {e}")
    
    return render_template('login.html')


@app.route('/logout')
def logout():
    """
    🚪 Logout user
    """
    session.clear()
    flash('Anda telah logout', 'success')
    return redirect(url_for('login'))


# =================== KELOLA USER (HANYA UP3) ===================
@app.route('/kelola_user', methods=['GET'])
def kelola_user():
    """
    👥 Kelola user - hanya untuk role UP3
    """
    if 'loggedin' not in session:
        return redirect(url_for('login'))
    
    if session.get('role') != 'UP3':
        flash('Akses ditolak. Hanya untuk user UP3.', 'danger')
        return redirect(url_for('dashboard_ulp'))
    
    try:
        query = text("""
            SELECT id_user, username, unitup, nama_ulp, role
            FROM tb_user
            ORDER BY unitup ASC
        """)
        
        users = pd.read_sql(query, engine)
        
    except Exception as e:
        flash(f'Gagal membaca data user: {str(e)}', 'danger')
        users = pd.DataFrame()
    
    return render_template(
        'kelola_user.html',
        users=users.to_dict('records') if not users.empty else []
    )


@app.route('/tambah_user', methods=['POST'])
def tambah_user():
    """
    ➕ Tambah user baru
    """
    if 'loggedin' not in session or session.get('role') != 'UP3':
        flash('Akses ditolak', 'danger')
        return redirect(url_for('login'))
    
    unitup = request.form.get('unitup', '').strip()
    nama_ulp = request.form.get('nama_ulp', '').strip()
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '')
    role = request.form.get('role', 'ULP')
    
    if not all([unitup, username, password]):
        flash('UNITUP, Username, dan Password harus diisi', 'danger')
        return redirect(url_for('kelola_user'))
    
    try:
        import hashlib
        hashed_pw = hashlib.sha256(password.encode()).hexdigest()
        
        with engine.begin() as conn:
            # Check if username already exists
            check = conn.execute(
                text("SELECT COUNT(*) FROM tb_user WHERE username = :username"),
                {'username': username}
            ).scalar()
            
            if check > 0:
                flash('Username sudah digunakan!', 'warning')
                return redirect(url_for('kelola_user'))
            
            # Check if unitup already exists (kecuali role UP3)
            if role == 'ULP':
                check_unitup = conn.execute(
                    text("SELECT COUNT(*) FROM tb_user WHERE unitup = :unitup"),
                    {'unitup': unitup}
                ).scalar()
                
                if check_unitup > 0:
                    flash('UNITUP sudah memiliki user!', 'warning')
                    return redirect(url_for('kelola_user'))
            
            # Insert user
            conn.execute(text("""
                INSERT INTO tb_user (unitup, nama_ulp, username, password, role)
                VALUES (:unitup, :nama_ulp, :username, :password, :role)
            """), {
                'unitup': unitup,
                'nama_ulp': nama_ulp,
                'username': username,
                'password': hashed_pw,
                'role': role
            })
            
        flash('User berhasil ditambahkan!', 'success')
        
    except Exception as e:
        flash(f'Gagal menambah user: {str(e)}', 'danger')
    
    return redirect(url_for('kelola_user'))


@app.route('/hapus_user/<int:id_user>')
def hapus_user(id_user):
    """
    🗑️ Hapus user
    """
    if 'loggedin' not in session or session.get('role') != 'UP3':
        flash('Akses ditolak', 'danger')
        return redirect(url_for('login'))
    
    try:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM tb_user WHERE id_user = :id_user"),
                {'id_user': id_user}
            )
        
        flash('User berhasil dihapus!', 'success')
        
    except Exception as e:
        flash(f'Gagal menghapus user: {str(e)}', 'danger')
    
    return redirect(url_for('kelola_user'))


# =================== MISSING IMPORT ===================
from flask import send_file


# =================== RUN APP ===================
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)



