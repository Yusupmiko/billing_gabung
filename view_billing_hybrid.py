from flask import render_template, request, redirect, url_for, session, flash, jsonify
from sqlalchemy import text
import pandas as pd
from markupsafe import escape
import logging
import traceback

logger = logging.getLogger(__name__)

# =================== VIEW BILLING HYBRID (Secure + Multi-Tab + Editable) ===================
@app.route('/view_billing', methods=['GET'])
def view_billing():
    """
    👁️ View data billing dengan:
    - Single table (billing)
    - Multi-tab navigation (5 tabs)
    - Editable form (dropdown + textarea)
    - Secure parameterized queries
    - Per-UNITUP access control
    """
    if 'loggedin' not in session:
        return redirect(url_for('login'))
    
    unitup = session.get('unitup')
    username = session.get('username')
    role = session.get('role', 'ULP')
    
    logger.info(f"📊 View billing accessed by {username} (UNITUP: {unitup}, Role: {role})")
    
    # ===== FILTER PARAMETERS =====
    active_tab = request.args.get('tab', 'dlpd_3bln')  # Default tab
    selected_blth = request.args.get('blth', '')
    selected_kdkelompok = request.args.get('kdkelompok', '')
    jam_nyala_min = request.args.get('jam_nyala_min', type=float, default=0)
    jam_nyala_max = request.args.get('jam_nyala_max', type=float, default=9999)
    
    # ===== BUILD BASE QUERY (SECURE) =====
    base_query = "SELECT * FROM billing WHERE 1=1"
    params = {}
    
    # Filter by UNITUP (Access Control)
    if role == 'ULP':
        # ULP hanya bisa lihat data UNITUP sendiri
        base_query += " AND unitup = :unitup"
        params['unitup'] = unitup
        logger.info(f"🔒 ULP access: filtered to UNITUP {unitup}")
    else:
        # UP3 bisa pilih UNITUP atau lihat semua
        unitup_filter = request.args.get('unitup_filter')
        if unitup_filter:
            base_query += " AND unitup = :unitup"
            params['unitup'] = unitup_filter
            logger.info(f"🔓 UP3 access: viewing UNITUP {unitup_filter}")
        else:
            logger.info(f"🔓 UP3 access: viewing ALL UNITUP")
    
    # Filter by BLTH (wajib untuk performa)
    if selected_blth:
        base_query += " AND blth = :blth"
        params['blth'] = selected_blth
    else:
        # Ambil BLTH terbaru otomatis
        try:
            latest_blth_query = "SELECT MAX(blth) as latest FROM billing WHERE 1=1"
            if role == 'ULP':
                latest_blth_query += " AND unitup = :unitup"
            
            latest_result = pd.read_sql(
                text(latest_blth_query), 
                engine, 
                params={'unitup': unitup} if role == 'ULP' else {}
            )
            
            if not latest_result.empty and latest_result.iloc[0]['latest']:
                selected_blth = str(latest_result.iloc[0]['latest'])
                base_query += " AND blth = :blth"
                params['blth'] = selected_blth
                logger.info(f"📅 Auto-selected latest BLTH: {selected_blth}")
        except Exception as e:
            logger.error(f"Error getting latest BLTH: {e}")
            flash("Silakan pilih periode BLTH", "warning")
    
    # Filter by KDKELOMPOK (opsional tapi recommended)
    if selected_kdkelompok:
        base_query += " AND kdkelompok = :kdkelompok"
        params['kdkelompok'] = selected_kdkelompok
        logger.info(f"📋 Filtered by KDKELOMPOK: {selected_kdkelompok}")
    
    # ===== QUERY BERDASARKAN TAB AKTIF (LAZY LOADING) =====
    try:
        # Tab-specific conditions
        tab_conditions = {
            'dlpd_3bln': " AND DLPD_3BLN = :dlpd_value",
            'naik': " AND ket = :ket_value",
            'turun': " AND ket = :ket_value",
            'div': " AND ket = :ket_value",
            'jam_nyala': " AND jam_nyala BETWEEN :min_jn AND :max_jn"
        }
        
        # Add tab-specific parameters
        if active_tab == 'dlpd_3bln':
            base_query += tab_conditions['dlpd_3bln']
            params['dlpd_value'] = 'Naik50% R3BLN'
        
        elif active_tab in ['naik', 'turun', 'div']:
            base_query += tab_conditions[active_tab]
            ket_map = {'naik': 'NAIK', 'turun': 'TURUN', 'div': 'DIV/NA'}
            params['ket_value'] = ket_map[active_tab]
        
        elif active_tab == 'jam_nyala':
            base_query += tab_conditions['jam_nyala']
            params['min_jn'] = jam_nyala_min
            params['max_jn'] = jam_nyala_max
        
        # Add sorting and limit
        base_query += " ORDER BY idpel ASC LIMIT 1000"
        
        # Execute query
        logger.info(f"🔍 Executing query for tab: {active_tab}")
        logger.debug(f"Query: {base_query}")
        logger.debug(f"Params: {params}")
        
        df_data = pd.read_sql(text(base_query), engine, params=params)
        
        logger.info(f"✅ Retrieved {len(df_data)} records for tab '{active_tab}'")
        
        # ===== GET DROPDOWN OPTIONS =====
        # BLTH list
        blth_query = "SELECT DISTINCT blth FROM billing"
        blth_params = {}
        if role == 'ULP':
            blth_query += " WHERE unitup = :unitup"
            blth_params['unitup'] = unitup
        blth_query += " ORDER BY blth DESC"
        
        blth_list = pd.read_sql(text(blth_query), engine, params=blth_params)['blth'].tolist()
        
        # KDKELOMPOK list
        kdkelompok_query = "SELECT DISTINCT kdkelompok FROM billing WHERE kdkelompok IS NOT NULL"
        kdkelompok_params = {}
        if role == 'ULP':
            kdkelompok_query += " AND unitup = :unitup"
            kdkelompok_params['unitup'] = unitup
        kdkelompok_query += " ORDER BY kdkelompok"
        
        kdkelompok_list = pd.read_sql(
            text(kdkelompok_query), 
            engine, 
            params=kdkelompok_params
        )['kdkelompok'].tolist()
        
        # UNITUP list (hanya untuk UP3)
        unitup_list = []
        if role == 'UP3':
            unitup_list = pd.read_sql(
                text("SELECT DISTINCT unitup FROM billing ORDER BY unitup"),
                engine
            )['unitup'].tolist()
        
        # ===== CREATE EDITABLE HTML TABLE =====
        table_html = create_editable_table(df_data, active_tab)
        
        # Count per tab (untuk badge)
        tab_counts = get_tab_counts(params, role, unitup)
        
    except Exception as e:
        logger.error(f"❌ Error in view_billing: {str(e)}")
        logger.error(traceback.format_exc())
        flash(f'Gagal membaca data: {str(e)}', 'danger')
        
        df_data = pd.DataFrame()
        blth_list = []
        kdkelompok_list = []
        unitup_list = []
        table_html = "<p class='text-center text-muted'>Tidak ada data</p>"
        tab_counts = {}
    
    return render_template(
        'view_billing.html',
        table_html=table_html,
        active_tab=active_tab,
        selected_blth=selected_blth,
        selected_kdkelompok=selected_kdkelompok,
        jam_nyala_min=jam_nyala_min,
        jam_nyala_max=jam_nyala_max,
        blth_list=blth_list,
        kdkelompok_list=kdkelompok_list,
        unitup_list=unitup_list,
        unitup_filter=request.args.get('unitup_filter', ''),
        role=role,
        tab_counts=tab_counts,
        total_records=len(df_data)
    )


# =================== CREATE EDITABLE TABLE ===================
def create_editable_table(df, active_tab):
    """
    📝 Convert DataFrame to editable HTML table
    """
    if df.empty:
        return "<p class='text-center text-muted py-4'>Tidak ada data untuk ditampilkan</p>"
    
    try:
        df_display = df.copy()
        
        # ===== HASIL PEMERIKSAAN DROPDOWN =====
        hasil_options = [
            "SESUAI", "TEMPER NYALA", "SALAH STAN", "SALAH FOTO", "FOTO BURAM",
            "ANOMALI PDL", "LEBIH TAGIH", "KURANG TAGIH", "BKN FOTO KWH",
            "BENCANA", "3BLN TANPA STAN", "BACA ULANG", "MASUK 720JN"
        ]
        
        hasil_dropdowns = []
        for _, row in df.iterrows():
            current_value = str(row.get('HASIL_PEMERIKSAAN', ''))
            
            options_html = '<option value="" selected hidden>-- Pilih --</option>'
            for opt in hasil_options:
                selected = 'selected' if current_value == opt else ''
                options_html += f'<option value="{opt}" {selected}>{opt}</option>'
            
            dropdown = f'''
                <select name="hasil_pemeriksaan_{row["IDPEL"]}" 
                        class="form-select form-select-sm hasil-pemeriksaan-select"
                        data-idpel="{row["IDPEL"]}"
                        data-blth="{row["BLTH"]}">
                    {options_html}
                </select>
            '''
            hasil_dropdowns.append(dropdown)
        
        df_display['HASIL_PEMERIKSAAN'] = hasil_dropdowns
        
        # ===== STAN VERIFIKASI TEXTAREA =====
        stan_textareas = []
        for _, row in df.iterrows():
            value = row.get('STAN_VERIFIKASI', '')
            if pd.isna(value):
                value = ''
            
            textarea = f'''
                <textarea name="stan_verifikasi_{row["IDPEL"]}" 
                          class="form-control form-control-sm stan-verifikasi-textarea"
                          rows="1"
                          data-idpel="{row["IDPEL"]}"
                          data-blth="{row["BLTH"]}"
                          placeholder="Stan verifikasi...">{escape(str(value))}</textarea>
            '''
            stan_textareas.append(textarea)
        
        df_display['STAN_VERIFIKASI'] = stan_textareas
        
        # ===== TINDAK LANJUT TEXTAREA =====
        tindak_lanjut_textareas = []
        for _, row in df.iterrows():
            value = row.get('TINDAK_LANJUT', '')
            if pd.isna(value):
                value = ''
            
            textarea = f'''
                <textarea name="tindak_lanjut_{row["IDPEL"]}" 
                          class="form-control form-control-sm tindak-lanjut-textarea"
                          rows="2"
                          data-idpel="{row["IDPEL"]}"
                          data-blth="{row["BLTH"]}"
                          placeholder="Tindak lanjut...">{escape(str(value))}</textarea>
            '''
            tindak_lanjut_textareas.append(textarea)
        
        df_display['TINDAK_LANJUT'] = tindak_lanjut_textareas
        
        # ===== CONVERT TO HTML =====
        table_html = df_display.to_html(
            classes="table table-striped table-hover table-sm table-bordered",
            index=False,
            escape=False,
            na_rep='',
            table_id="billingTable"
        )
        
        return table_html
        
    except Exception as e:
        logger.error(f"Error creating editable table: {str(e)}")
        logger.error(traceback.format_exc())
        return f"<p class='text-danger'>Error: {str(e)}</p>"


# =================== GET TAB COUNTS ===================
def get_tab_counts(base_params, role, unitup):
    """
    🔢 Get record counts for each tab (untuk badge notifikasi)
    """
    try:
        counts = {}
        
        # Base WHERE untuk semua tab
        where_clause = "WHERE 1=1"
        params = base_params.copy()
        
        if role == 'ULP':
            where_clause += " AND unitup = :unitup"
            params['unitup'] = unitup
        
        if 'blth' in params:
            where_clause += " AND blth = :blth"
        
        if 'kdkelompok' in params:
            where_clause += " AND kdkelompok = :kdkelompok"
        
        # Count per tab
        tab_queries = {
            'dlpd_3bln': f"SELECT COUNT(*) as cnt FROM billing {where_clause} AND DLPD_3BLN = 'Naik50% R3BLN'",
            'naik': f"SELECT COUNT(*) as cnt FROM billing {where_clause} AND ket = 'NAIK'",
            'turun': f"SELECT COUNT(*) as cnt FROM billing {where_clause} AND ket = 'TURUN'",
            'div': f"SELECT COUNT(*) as cnt FROM billing {where_clause} AND ket = 'DIV/NA'",
            'jam_nyala': f"SELECT COUNT(*) as cnt FROM billing {where_clause} AND jam_nyala > 600"
        }
        
        for tab_name, query in tab_queries.items():
            result = pd.read_sql(text(query), engine, params=params)
            counts[tab_name] = int(result.iloc[0]['cnt']) if not result.empty else 0
        
        logger.info(f"📊 Tab counts: {counts}")
        return counts
        
    except Exception as e:
        logger.error(f"Error getting tab counts: {str(e)}")
        return {}


# =================== SAVE EDITABLE DATA (AJAX ENDPOINT) ===================
@app.route('/save_billing_edit', methods=['POST'])
def save_billing_edit():
    """
    💾 Save edited data dari form (dipanggil via AJAX)
    """
    if 'loggedin' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        updates = data.get('updates', [])
        username = session.get('username')
        
        if not updates:
            return jsonify({'error': 'Tidak ada data untuk disimpan'}), 400
        
        success_count = 0
        failed_count = 0
        
        with engine.begin() as conn:
            for item in updates:
                try:
                    sql = text("""
                        UPDATE billing 
                        SET 
                            hasil_pemeriksaan = :hasil,
                            stan_verifikasi = :stan,
                            tindak_lanjut = :tindak_lanjut,
                            updated_by = :username,
                            updated_at = NOW()
                        WHERE idpel = :idpel AND blth = :blth
                    """)
                    
                    result = conn.execute(sql, {
                        'hasil': item.get('hasil_pemeriksaan'),
                        'stan': item.get('stan_verifikasi'),
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
                    logger.error(f"Failed to save {item.get('idpel')}: {e}")
                    failed_count += 1
                    continue
        
        logger.info(f"✅ Saved: {success_count}, Failed: {failed_count}")
        
        return jsonify({
            'success': True,
            'message': f'Berhasil menyimpan {success_count} data',
            'saved': success_count,
            'failed': failed_count
        })
        
    except Exception as e:
        logger.error(f"Error saving edits: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500