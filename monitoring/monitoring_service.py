"""
Monitoring Service Module with UNITUP and BLTH filtering
Handles all monitoring-related business logic and database operations
"""

class MonitoringService:
    def __init__(self, db_connection_func, unitup_filter=None, blth_filter=None):
        """
        Initialize monitoring service
        Args:
            db_connection_func: Function that returns database connection
            unitup_filter: UNITUP code for filtering (None = no filter/admin)
            blth_filter: BLTH code for filtering (None = no filter)
        """
        self.get_db_connection = db_connection_func
        self.unitup_filter = unitup_filter
        self.blth_filter = blth_filter
    
    def _add_filters(self, query, params=None):
        """Add UNITUP and BLTH filters to query if needed"""
        if params is None:
            params = []
        
        if self.unitup_filter:
            query += " AND UNITUP = %s"
            params.append(self.unitup_filter)
        
        if self.blth_filter:
            query += " AND BLTH = %s"
            params.append(self.blth_filter)
        
        return query, params
    
    # ==================== STATUS RECAP FUNCTIONS ====================
    
    def fetch_status_recap(self, table, filter_marking=False):
        """
        Fetch status recap for a single table
        Args:
            table: Table name
            filter_marking: Boolean to filter by DLPD_HITUNG
        Returns:
            List of dict with status and count
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        base_query = f"""
            SELECT `HASIL_PEMERIKSAAN` AS status, COUNT(*) AS jumlah
            FROM {table}
            WHERE KET IN ('NAIK', 'TURUN', 'DIV/NA') 
              AND (KDKELOMPOK IN ('1','2','3','4','5','6','7','8','P'))
        """
        params = []
        
        if filter_marking:
            base_query += " AND `DLPD_HITUNG` IS NOT NULL AND `DLPD_HITUNG` <> ''"
        
        base_query, params = self._add_filters(base_query, params)
        base_query += " GROUP BY `HASIL_PEMERIKSAAN`"
        
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    
    def combine_recap_data(self, all_data):
        """
        Combine recap data from multiple tables
        Args:
            all_data: List of results from fetch_status_recap
        Returns:
            Tuple of (recap_list, total)
        """
        combined = {}
        total = 0
        
        for data in all_data:
            for row in data:
                status = row['status'] if row['status'] else 'BELUM DIISI'
                if status == 'AMAN':
                    continue
                jumlah = row['jumlah']
                combined[status] = combined.get(status, 0) + jumlah
                total += jumlah
        
        recap_list = []
        for status, jumlah in combined.items():
            perc = round((jumlah / total) * 100, 1) if total > 0 else 0
            recap_list.append({
                'status': status,
                'jumlah': jumlah,
                'persentase': perc
            })
        
        # Sort by jumlah descending
        recap_list = sorted(recap_list, key=lambda x: x['jumlah'], reverse=True)
        return recap_list, total
    
    # ==================== PIVOT FUNCTIONS ====================
    
    def generate_pivot_status(self, table, filter_marking=False):
        """
        Generate pivot table by BLTH, KDKELOMPOK, and status
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        base_query = f"""
            SELECT BLTH, KDKELOMPOK, `HASIL_PEMERIKSAAN` AS status, COUNT(*) AS jumlah
            FROM {table}
            WHERE KET IN ('NAIK', 'TURUN', 'DIV/NA','AMAN')
              AND (KDKELOMPOK IN ('1','2','3','4','5','6','7','8','P'))
        """
        params = []
        
        if filter_marking:
            base_query += " AND `DLPD_HITUNG` IS NOT NULL AND LENGTH(TRIM(`DLPD_HITUNG`)) > 0"
        
        base_query, params = self._add_filters(base_query, params)
        base_query += " GROUP BY BLTH, KDKELOMPOK, `HASIL_PEMERIKSAAN`"
        
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        pivot = {}
        statuses = set()
        
        for row in results:
            status = row['status'] if row['status'] else 'BELUM DIISI'
            label = f"{row['BLTH']} - {row['KDKELOMPOK']}"
            jumlah = row['jumlah']
            statuses.add(status)
            
            if label not in pivot:
                pivot[label] = {}
            pivot[label][status] = pivot[label].get(status, 0) + jumlah
        
        return pivot, sorted(statuses)
    
    def generate_pivot_status_koreksi(self, table, filter_marking=False):
        """
        Generate pivot for correction data (MARKING_KOREKSI > 0)
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        base_query = f"""
            SELECT BLTH, KDKELOMPOK, `HASIL_PEMERIKSAAN` AS status, COUNT(*) AS jumlah
            FROM `{table}`
            WHERE MARKING_KOREKSI > 0
              AND KDKELOMPOK IN ('1','2','3','4','5','6','7','8','P')
        """
        params = []
        
        base_query, params = self._add_filters(base_query, params)
        base_query += " GROUP BY BLTH, KDKELOMPOK, `HASIL_PEMERIKSAAN`"
        
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        pivot = {}
        statuses = set()
        
        for row in results:
            status = row['status'] if row['status'] else 'BELUM DIISI'
            label = f"{row['BLTH']} - {row['KDKELOMPOK']}"
            jumlah = row['jumlah']
            statuses.add(status)
            
            if label not in pivot:
                pivot[label] = {}
            pivot[label][status] = pivot[label].get(status, 0) + jumlah
        
        return pivot, sorted(statuses)
    
    def generate_pivot_dlpd_hitung(self, table, filter_marking=True):
        """
        Generate pivot by DLPD_HITUNG categories
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()

        base_query = f"""
            SELECT `DLPD_HITUNG`, `HASIL_PEMERIKSAAN` AS status, COUNT(*) AS jumlah
            FROM {table}
            WHERE KET IN ('NAIK', 'TURUN', 'DIV/NA','AMAN')
            AND KDKELOMPOK IN ('1','2','3','4','5','6','7','8','P')
        """
        params = []

        if filter_marking:
            base_query += " AND `DLPD_HITUNG` IS NOT NULL AND LENGTH(TRIM(`DLPD_HITUNG`)) > 0"

        base_query, params = self._add_filters(base_query, params)
        base_query += " GROUP BY `DLPD_HITUNG`, `HASIL_PEMERIKSAAN`"

        cursor.execute(base_query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        # 🔁 Normalisasi nama DLPD agar seragam dengan dashboard
        dlpd_mapping = {
            'JN>720': 'JN_720up',
            'JN<40': 'JN_40Down',
            'PECAHAN': 'Cek_Pecahan',
            'DIV/NA': 'Cek_DIV/NA',
            'NAIK>50%': 'Naik_50%Up',
            'KWH NOL': 'kWh_Nol',
            'STAN MUNDUR': 'Stan_Mundur',
            'TURUN<50%': 'TURUN<50%'
        }

        # Daftar kategori (baris pivot) default
        rows = [
            'JN_720up',
            'Cek_Pecahan',
            'Stan_Mundur',
            'Naik_50%Up',
            'Cek_DIV/NA',
            'TURUN<50%',
            'kWh_Nol',
            'JN_40Down'
        ]

        # ✅ PERBAIKAN: Buat struktur pivot kosong untuk SEMUA rows
        pivot = {r: {} for r in rows}
        statuses = set()
        
        # ✅ Buat normalized mapping untuk lookup cepat
        normalized_mapping = {k.strip().upper(): v for k, v in dlpd_mapping.items()}

        # ✅ Proses data dari database
        for row in results:
            dlpd_raw = row['DLPD_HITUNG']
            
            # Normalisasi: trim whitespace dan uppercase untuk matching
            dlpd_normalized = dlpd_raw.strip().upper() if dlpd_raw else ''
            
            # Cari mapping yang sesuai
            mapped_dlpd = normalized_mapping.get(dlpd_normalized, dlpd_raw)
            
            status = row['status'] if row['status'] else 'BELUM DIISI'
            jumlah = row['jumlah']
            statuses.add(status)

            # ✅ CRITICAL: Pastikan mapped_dlpd ada di pivot
            # Jika tidak ada, tambahkan (untuk data yang tidak ada di rows list)
            if mapped_dlpd not in pivot:
                pivot[mapped_dlpd] = {}
                print(f"⚠️ Added new category to pivot: '{mapped_dlpd}' (from '{dlpd_raw}')")

            # Tambahkan atau update jumlah
            if status in pivot[mapped_dlpd]:
                pivot[mapped_dlpd][status] += jumlah
            else:
                pivot[mapped_dlpd][status] = jumlah
            
            print(f"✅ Mapped: '{dlpd_raw}' -> '{mapped_dlpd}' | Status: {status} | Jumlah: {jumlah}")

        # ✅ DEBUG: Print final pivot
        print("\n=== FINAL PIVOT (generate_pivot_dlpd_hitung) ===")
        for dlpd in rows:  # Print dalam urutan yang benar
            data = pivot.get(dlpd, {})
            total = sum(data.values())
            print(f"{dlpd}: {data} (Total: {total})")

        return pivot, sorted(statuses)


    def generate_pivot_ganda(self, table):
        """
        Generate pivot for duplicate customers (KDKELOMPOK I and A)
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        base_query = f"""
            SELECT 
                COALESCE(`DLPD_HITUNG`, '') AS DLPD_HITUNG, 
                COALESCE(`HASIL_PEMERIKSAAN`, '') AS status, 
                COUNT(*) AS jumlah
            FROM {table}
            WHERE KET IN ('NAIK', 'TURUN', 'DIV/NA','AMAN')
            AND KDKELOMPOK IN ('I','A')
        """
        params = []
        
        base_query, params = self._add_filters(base_query, params)
        base_query += " GROUP BY DLPD_HITUNG, `HASIL_PEMERIKSAAN`"
        
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # ✅ PERBAIKAN: Gunakan mapping yang sama seperti generate_pivot_dlpd_hitung
        dlpd_mapping = {
            'JN>720': 'JN_720up',
            'JN<40': 'JN_40Down',
            'PECAHAN': 'Cek_Pecahan',
            'DIV/NA': 'Cek_DIV/NA',
            'NAIK>50%': 'Naik_50%Up',
            'KWH NOL': 'kWh_Nol',
            'STAN MUNDUR': 'Stan_Mundur',
            'TURUN<50%': 'TURUN<50%'
        }
        
        rows = [
            'JN_720up', 
            'Cek_Pecahan', 
            'Stan_Mundur', 
            'Naik_50%Up',
            'Cek_DIV/NA', 
            'TURUN<50%',  # ✅ PERBAIKAN: Ganti dari 'Turun_50%Down'
            'kWh_Nol', 
            'JN_40Down', 
            ''  # Untuk data kosong
        ]
        
        pivot = {r: {} for r in rows}
        statuses = set()
        
        # ✅ Buat normalized mapping untuk lookup cepat
        normalized_mapping = {k.strip().upper(): v for k, v in dlpd_mapping.items()}
        
        for row in results:
            dlpd_raw = row['DLPD_HITUNG'] or ''
            
            # ✅ PERBAIKAN: Tambahkan normalisasi seperti di generate_pivot_dlpd_hitung
            if dlpd_raw:
                dlpd_normalized = dlpd_raw.strip().upper()
                mapped_dlpd = normalized_mapping.get(dlpd_normalized, dlpd_raw)
            else:
                mapped_dlpd = ''
            
            status = row['status'] or 'BELUM DIISI'
            jumlah = row['jumlah']
            statuses.add(status)
            
            # ✅ Pastikan mapped_dlpd ada di pivot
            if mapped_dlpd not in pivot:
                pivot[mapped_dlpd] = {}
                print(f"⚠️ [GANDA] Added new category: '{mapped_dlpd}' (from '{dlpd_raw}')")
            
            # Tambahkan atau update jumlah
            if status in pivot[mapped_dlpd]:
                pivot[mapped_dlpd][status] += jumlah
            else:
                pivot[mapped_dlpd][status] = jumlah
            
            print(f"✅ [GANDA] Mapped: '{dlpd_raw}' -> '{mapped_dlpd}' | Status: {status} | Jumlah: {jumlah}")
        
        # ✅ DEBUG: Print final pivot
        print("\n=== FINAL PIVOT (generate_pivot_ganda) ===")
        for dlpd in rows:
            data = pivot.get(dlpd, {})
            total = sum(data.values())
            if total > 0 or dlpd == '':  # Print jika ada data atau kategori kosong
                print(f"{dlpd if dlpd else '(Kosong)'}: {data} (Total: {total})")
        
        return pivot, sorted(statuses)


    def get_dlpd_reverse_mapping(self):
        """
        Get reverse mapping from display name to database name
        """
        dlpd_mapping = {
            'JN>720': 'JN_720up',
            'JN<40': 'JN_40Down',
            'PECAHAN': 'Cek_Pecahan',
            'DIV/NA': 'Cek_DIV/NA',
            'NAIK>50%': 'Naik_50%Up',
            'KWH NOL': 'kWh_Nol',
            'STAN MUNDUR': 'Stan_Mundur', 
            'TURUN<50%': 'TURUN<50%' 
        }
        # Membalikkan mapping: {frontend_label: db_value}
        return {v: k for k, v in dlpd_mapping.items()}
    
        # Di file monitoring_service.py
    # Tambahkan di dalam class MonitoringService (sekitar baris 350)

    # def get_detail_pelanggan_dlpd_hb(self, table, blth, kdkelompok, hasil_pemeriksaan=None, 
    #                                 dlpd_hitung=None, filter_marking=False):
    #     """
    #     Get detailed customer data including AMAN status for DLPD with Hari Baca
    #     """
    #     conn = self.get_db_connection()
    #     cursor = conn.cursor()
        
    #     query = f"""
    #         SELECT * FROM {table}
    #         WHERE BLTH = %s AND KDKELOMPOK = %s
    #         AND DLPD_HITUNG IS NOT NULL AND LENGTH(TRIM(DLPD_HITUNG)) > 0
    #         AND KET IN ('NAIK', 'TURUN', 'DIV/NA', 'AMAN')
    #     """
    #     params = [blth, kdkelompok]
        
    #     if hasil_pemeriksaan is not None:
    #         if hasil_pemeriksaan == 'BELUM DIISI':
    #             query += " AND (`HASIL_PEMERIKSAAN` IS NULL OR `HASIL_PEMERIKSAAN` = '')"
    #         else:
    #             query += " AND `HASIL_PEMERIKSAAN` = %s"
    #             params.append(hasil_pemeriksaan)
        
    #     if dlpd_hitung:
    #         query += " AND DLPD_HITUNG = %s"
    #         params.append(dlpd_hitung)
        
    #     # Add UNITUP and BLTH filters
    #     query, params = self._add_filters(query, params)
        
    #     cursor.execute(query, params)
    #     data = cursor.fetchall()
    #     columns = [desc[0] for desc in cursor.description]
        
    #     cursor.close()
    #     conn.close()
        
    #     return {'columns': columns, 'data': data}
    
        # Di dalam class MonitoringService (monitoring_service.py)

    def get_detail_pelanggan_dlpd_hb(self, table, blth, kdkelompok, hasil_pemeriksaan=None, 
                                    dlpd_hitung=None, filter_marking=False):
        """
        Get detailed customer data including AMAN status for DLPD with Hari Baca
        
        Args:
            table: Table name (billing)
            blth: Bulan/tahun
            kdkelompok: Kelompok pelanggan
            hasil_pemeriksaan: Status pemeriksaan (optional)
            dlpd_hitung: DLPD value (optional)
            filter_marking: Whether to filter by DLPD_HITUNG
        
        Returns:
            Dict with columns and data
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        # ✅ Query dengan nama kolom yang benar sesuai database
        query = f"""
            SELECT 
                BLTH,
                UNITUP,
                IDPEL,
                NAMA,
                TARIF,
                DAYA,
                SLALWBP,
                LWBPCABUT,
                SELISIH_STAN_BONGKAR,
                LWBPPASANG,
                KWH_SEKARANG,
                KWH_1_BULAN_LALU,
                KWH_2_BULAN_LALU,
                SAHLWBP,
                DELTA_PEMKWH,
                PERSEN,
                JAM_NYALA,
                JAMNYALA600,
                NOMORKWH,
                GRAFIK,
                FOTO_AKHIR,
                FOTO_LALU,
                FOTO_LALU2,
                FOTO_3BLN,
                HASIL_PEMERIKSAAN,
                STAN_VERIFIKASI,
                TINDAK_LANJUT,
                KET,
                KDKELOMPOK,
                DLPD,
                DLPD_3BLN,
                DLPD_HITUNG,
                MARKING_KOREKSI
            FROM {table}
            WHERE BLTH = %s 
            AND KDKELOMPOK = %s
            AND DLPD_HITUNG IS NOT NULL 
            AND LENGTH(TRIM(DLPD_HITUNG)) > 0
            AND KET IN ('NAIK', 'TURUN', 'DIV/NA', 'AMAN')
        """
        params = [blth, kdkelompok]
        
        # Add hasil pemeriksaan filter
        if hasil_pemeriksaan is not None:
            if hasil_pemeriksaan == 'BELUM DIISI' or hasil_pemeriksaan == '':
                query += " AND (HASIL_PEMERIKSAAN IS NULL OR HASIL_PEMERIKSAAN = '')"
            else:
                query += " AND HASIL_PEMERIKSAAN = %s"
                params.append(hasil_pemeriksaan)
        
        # Add DLPD filter
        if dlpd_hitung:
            query += " AND DLPD_HITUNG = %s"
            params.append(dlpd_hitung)
        
        # ✅ Add UNITUP and BLTH filters from MonitoringService instance
        query, params = self._add_filters(query, params)
        
        print(f"[SQL DEBUG] Query: {query}")
        print(f"[SQL DEBUG] Params: {params}")
        
        try:
            cursor.execute(query, params)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            print(f"[SQL DEBUG] Fetched {len(data)} rows")
            
            cursor.close()
            conn.close()
            
            return {'columns': columns, 'data': data}
            
        except Exception as e:
            print(f"[SQL ERROR] {str(e)}")
            cursor.close()
            conn.close()
            raise


    def _add_filters(self, query, params=None):
        """
        Add UNITUP and BLTH filters to query if needed
        
        Args:
            query: SQL query string
            params: List of parameters
        
        Returns:
            Tuple of (modified_query, modified_params)
        """
        if params is None:
            params = []
        
        # Add UNITUP filter if specified (untuk non-admin atau admin dengan filter)
        if self.unitup_filter:
            query += " AND UNITUP = %s"
            params.append(self.unitup_filter)
            print(f"[FILTER] Adding UNITUP filter: {self.unitup_filter}")
        
        # Add BLTH filter if specified
        if self.blth_filter:
            # Jangan tambahkan filter BLTH lagi jika sudah ada di WHERE clause
            if "WHERE BLTH = %s" not in query and "AND BLTH = %s" not in query:
                query += " AND BLTH = %s"
                params.append(self.blth_filter)
                print(f"[FILTER] Adding BLTH filter: {self.blth_filter}")
        
        return query, params
        
    
    # ==================== KET SUMMARY FUNCTIONS ====================
    
    def fetch_ket_summary(self, table, filter_marking=False):
        """
        Fetch KET summary (NAIK, TURUN, DIV/NA)
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        base_query = f"""
            SELECT KET, COUNT(*) AS jumlah
            FROM {table}
            WHERE KET IN ('NAIK', 'TURUN', 'DIV/NA')
              AND KDKELOMPOK IN ('1','2','3','4','5','6','7','8','P')
        """
        params = []
        
        if filter_marking:
            base_query += " AND DLPD_HITUNG IS NOT NULL AND LENGTH(TRIM(DLPD_HITUNG)) > 0"
        
        base_query, params = self._add_filters(base_query, params)
        base_query += " GROUP BY KET"
        
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    
    def summarize_ket_grouped(self, ket_data_list):
        """
        Combine KET summary from multiple tables
        """
        total = 0
        summary = {'NAIK': 0, 'TURUN': 0, 'DIV/NA': 0}
        
        for data in ket_data_list:
            for row in data:
                ket = row['KET']
                jumlah = row['jumlah']
                if ket in summary:
                    summary[ket] += jumlah
                    total += jumlah
        
        result = {}
        for ket in ['NAIK', 'TURUN', 'DIV/NA']:
            jumlah = summary[ket]
            persen = round((jumlah / total) * 100, 1) if total > 0 else 0
            result[ket] = {'jumlah': jumlah, 'persentase': persen}
        
        return result, total
    
    # ==================== DETAIL RETRIEVAL FUNCTIONS ====================
    
    def get_detail_pelanggan(self, table, blth, kdkelompok, hasil_pemeriksaan=None, 
                            dlpd_hitung=None, filter_marking=False):
        """
        Get detailed customer data based on filters
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        query = f"""
            SELECT * FROM {table}
            WHERE BLTH = %s AND KDKELOMPOK = %s
              AND KET IN ('NAIK', 'TURUN', 'DIV/NA')
        """
        params = [blth, kdkelompok]
        
        if filter_marking:
            query += " AND DLPD_HITUNG IS NOT NULL AND LENGTH(TRIM(DLPD_HITUNG)) > 0"
        
        if hasil_pemeriksaan is not None:
            if hasil_pemeriksaan == 'BELUM DIISI':
                query += " AND (`HASIL_PEMERIKSAAN` IS NULL OR `HASIL_PEMERIKSAAN` = '')"
            else:
                query += " AND `HASIL_PEMERIKSAAN` = %s"
                params.append(hasil_pemeriksaan)
        
        if dlpd_hitung:
            query += " AND DLPD_HITUNG = %s"
            params.append(dlpd_hitung)
        
        query, params = self._add_filters(query, params)
        
        cursor.execute(query, params)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        cursor.close()
        conn.close()
        
        return {'columns': columns, 'data': data}
    
    def get_detail_by_dlpd(self, table, dlpd_hitung, hasil_pemeriksaan=None, 
                          filter_marking=False, kdkelompok_list=None):
        """
        Get detailed customer data filtered by DLPD_HITUNG
        """
        if kdkelompok_list is None:
            kdkelompok_list = ['1','2','3','4','5','6','7','8','P']
        
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        kdkelompok_str = "','".join(kdkelompok_list)
        query = f"""
            SELECT * FROM {table}
            WHERE DLPD_HITUNG = %s
            AND KET IN ('NAIK', 'TURUN', 'DIV/NA', 'AMAN')
            AND KDKELOMPOK IN ('{kdkelompok_str}')
        """
        params = [dlpd_hitung]
        
        if hasil_pemeriksaan:
            query += " AND `HASIL_PEMERIKSAAN` = %s"
            params.append(hasil_pemeriksaan)
        else:
            query += " AND (`HASIL_PEMERIKSAAN` IS NULL OR `HASIL_PEMERIKSAAN` = '')"
        
        if filter_marking:
            query += " AND DLPD_HITUNG IS NOT NULL AND LENGTH(TRIM(DLPD_HITUNG)) > 0"
        
        query, params = self._add_filters(query, params)
        
        cursor.execute(query, params)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        cursor.close()
        conn.close()
        
        return {'columns': columns, 'data': data}
    
    def get_detail_koreksi(self, table, blth, kdkelompok, hasil_pemeriksaan=None):
        """
        Get correction data details
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        query = f"""
            SELECT * FROM {table}
            WHERE BLTH = %s AND KDKELOMPOK = %s AND MARKING_KOREKSI > 0
        """
        params = [blth, kdkelompok]
        
        if hasil_pemeriksaan is not None:
            if hasil_pemeriksaan == 'BELUM DIISI':
                query += " AND (`HASIL_PEMERIKSAAN` IS NULL OR `HASIL_PEMERIKSAAN` = '')"
            else:
                query += " AND `HASIL_PEMERIKSAAN` = %s"
                params.append(hasil_pemeriksaan)
        
        query, params = self._add_filters(query, params)
        
        cursor.execute(query, params)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        cursor.close()
        conn.close()
        
        return {'columns': columns, 'data': data}
    
    def search_customer_by_idpel(self, table, idpel):
        """
        Search customer by IDPEL
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        query = f"SELECT * FROM {table} WHERE IDPEL = %s"
        params = [idpel]
        
        query, params = self._add_filters(query, params)
        
        cursor.execute(query, params)
        data = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return data
    
    # ==================== UPDATE FUNCTIONS ====================
    
        
    def update_hasil_pemeriksaan(self, table, updates):
        """
        Update hasil pemeriksaan for multiple records
        Args:
            table: Table name
            updates: List of dict with keys: IDPEL, HASIL, TINDAK, STAN
        Returns:
            Dict with status and message
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            success_count = 0
            
            for row in updates:
                # Validate required fields
                if 'IDPEL' not in row:
                    print(f"[UPDATE SKIP] Missing IDPEL in row: {row}")
                    continue
                
                idpel = row.get('IDPEL')
                hasil = row.get('HASIL', row.get('HASIL_PEMERIKSAAN', ''))
                tindak = row.get('TINDAK', row.get('TINDAK_LANJUT', ''))
                stan = row.get('STAN', row.get('STAN_VERIFIKASI', ''))
                
                print(f"[UPDATE] Processing IDPEL: {idpel}")
                print(f"[UPDATE] HASIL: {hasil}, TINDAK: {tindak}, STAN: {stan}")
                
                update_query = f"""
                    UPDATE {table}
                    SET HASIL_PEMERIKSAAN = %s,
                        TINDAK_LANJUT = %s,
                        STAN_VERIFIKASI = %s
                    WHERE IDPEL = %s
                """
                update_params = [hasil, tindak, stan, idpel]
                
                # Add UNITUP filter for security
                if self.unitup_filter:
                    update_query += " AND UNITUP = %s"
                    update_params.append(self.unitup_filter)
                    print(f"[UPDATE] Adding UNITUP filter: {self.unitup_filter}")
                
                # Add BLTH filter for security
                if self.blth_filter:
                    update_query += " AND BLTH = %s"
                    update_params.append(self.blth_filter)
                    print(f"[UPDATE] Adding BLTH filter: {self.blth_filter}")
                
                print(f"[UPDATE] Query: {update_query}")
                print(f"[UPDATE] Params: {update_params}")
                
                cursor.execute(update_query, update_params)
                rows_affected = cursor.rowcount
                print(f"[UPDATE] Rows affected: {rows_affected}")
                
                success_count += rows_affected
            
            conn.commit()
            print(f"[UPDATE] Total rows updated: {success_count}")
            return {"status": "success", "message": f"Berhasil update {success_count} data"}
        
        except Exception as e:
            conn.rollback()
            print(f"[UPDATE ERROR] {str(e)}")
            import traceback
            traceback.print_exc()
            return {"status": "error", "message": str(e)}
        finally:
            cursor.close()
            conn.close()
            
# def get_detail_pelanggan_dlpd_hb(self, table, blth, kdkelompok, hasil_pemeriksaan=None, 
#                                   dlpd_hitung=None, filter_marking=False):
#     """
#     Get detailed customer data including AMAN status for DLPD with Hari Baca
#     """
#     conn = self.get_db_connection()
#     cursor = conn.cursor()
    
#     query = f"""
#         SELECT * FROM {table}
#         WHERE BLTH = %s AND KDKELOMPOK = %s
#           AND DLPD_HITUNG IS NOT NULL AND LENGTH(TRIM(DLPD_HITUNG)) > 0
#           AND KET IN ('NAIK', 'TURUN', 'DIV/NA', 'AMAN')
#     """
#     params = [blth, kdkelompok]
    
#     if hasil_pemeriksaan is not None:
#         if hasil_pemeriksaan == 'BELUM DIISI':
#             query += " AND (`HASIL_PEMERIKSAAN` IS NULL OR `HASIL_PEMERIKSAAN` = '')"
#         else:
#             query += " AND `HASIL_PEMERIKSAAN` = %s"
#             params.append(hasil_pemeriksaan)
    
#     if dlpd_hitung:
#         query += " AND DLPD_HITUNG = %s"
#         params.append(dlpd_hitung)
    
#     # Add UNITUP and BLTH filters
#     query, params = self._add_filters(query, params)
    
#     cursor.execute(query, params)
#     data = cursor.fetchall()
#     columns = [desc[0] for desc in cursor.description]
    
#     cursor.close()
#     conn.close()
    
#     return {'columns': columns, 'data': data}