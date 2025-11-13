$(document).ready(function () {
  // ================================================================
  // HELPER: Get URL Parameters
  // ================================================================
  function getUrlParams() {
    const urlParams = new URLSearchParams(window.location.search);
    return {
      unitup: urlParams.get('unitup') || '',
      blth: urlParams.get('blth') || ''
    };
  }

  // ================================================================
  // HANDLER 1: Detail Link DLPD
  // ================================================================
  $(document).on("click", ".detail-link_dlpd", function (e) {
    e.preventDefault();

    const params = getUrlParams();
    const dlpd_hitung = $(this).data("dlpd");
    const hasil_pemeriksaan = $(this).data("hasilpemeriksaan") || "";
    const table = $(this).data("table");

    console.log("🔍 Link DLPD clicked:", {
      dlpd: dlpd_hitung,
      hasil: hasil_pemeriksaan,
      table: table,
      unitup: params.unitup,
      blth: params.blth
    });

    if (!table || !dlpd_hitung) {
      alert("❌ Parameter table dan DLPD wajib ada");
      return;
    }

    $("#detailModal").attr("data-table", table);
    $("#detailTable").attr("data-table", table);
    $("#detailModalLabel").text(
      `Detail Pelanggan ${dlpd_hitung} (Status: ${hasil_pemeriksaan || "BELUM DIISI"})`
    );
    $("#detailTableHead, #detailTableBody").empty();

    $.ajax({
      url: "/monitoring/get_detail_pelanggan_dlpd",
      method: "GET",
      data: {
        dlpd_hitung: dlpd_hitung,
        hasil_pemeriksaan: hasil_pemeriksaan,
        table: table,
        unitup: params.unitup,  // ✅ ADDED
        blth: params.blth       // ✅ ADDED
      },
      success: function (response) {
        console.log("✅ Data received:", response);
        renderModalTable(response.data, "dlpd");
      },
      error: function (xhr) {
        console.error("❌ AJAX Error:", xhr.responseText);
        const errorMsg = xhr.responseJSON?.error || xhr.statusText || "Unknown error";
        $("#detailTableBody").html(
          '<tr><td colspan="18" class="text-center text-danger">Gagal memuat data: ' +
            errorMsg +
            "</td></tr>"
        );
        const modal = new bootstrap.Modal(document.getElementById("detailModal"));
        modal.show();
      },
    });
  });

  // ================================================================
  // HANDLER 2: Detail Link GANDA
  // ================================================================
  $(document).on("click", ".detail-link_ganda", function (e) {
    e.preventDefault();

    const params = getUrlParams();
    const dlpd_hitung = $(this).data("dlpd");
    const hasil_pemeriksaan = $(this).data("hasilpemeriksaan") || "";
    const table = $(this).data("table");

    console.log("🔍 Link GANDA clicked:", {
      dlpd: dlpd_hitung,
      hasil: hasil_pemeriksaan,
      table: table,
      unitup: params.unitup,
      blth: params.blth
    });

    if (!table || !dlpd_hitung) {
      alert("❌ Parameter table dan DLPD wajib ada");
      return;
    }

    $("#detailModal").attr("data-table", table);
    $("#detailTable").attr("data-table", table);
    $("#detailModalLabel").text(
      `Detail Pelanggan ${dlpd_hitung} (Status: ${hasil_pemeriksaan || "BELUM DIISI"})`
    );
    $("#detailTableHead, #detailTableBody").empty();

    $.ajax({
      url: "/monitoring/get_detail_pelanggan_ganda",
      method: "GET",
      data: {
        dlpd_hitung: dlpd_hitung,
        hasil_pemeriksaan: hasil_pemeriksaan,
        table: table,
        unitup: params.unitup,  // ✅ ADDED
        blth: params.blth       // ✅ ADDED
      },
      success: function (response) {
        console.log("✅ Data received:", response);
        renderModalTable(response.data, "ganda");
      },
      error: function (xhr) {
        console.error("❌ AJAX Error:", xhr.responseText);
        const errorMsg = xhr.responseJSON?.error || xhr.statusText || "Unknown error";
        $("#detailTableBody").html(
          '<tr><td colspan="18" class="text-center text-danger">Gagal memuat data: ' +
            errorMsg +
            "</td></tr>"
        );
        const modal = new bootstrap.Modal(document.getElementById("detailModal"));
        modal.show();
      },
    });
  });

  // ================================================================
  // HANDLER 3: Detail Link KOREKSI
  // ================================================================
  $(document).on("click", ".detail-link_koreksi", function (e) {
    e.preventDefault();

    const params = getUrlParams();
    const blth = $(this).data("blth");
    const kdkelompok = $(this).data("kdkelompok");
    const hasil_pemeriksaan = $(this).data("hasilpemeriksaan");
    const table = $(this).data("table");
    const dlpd_hitung = $(this).data("dlpd");

    console.log("🔍 Link KOREKSI clicked:", {
      blth: blth,
      kdkelompok: kdkelompok,
      hasil: hasil_pemeriksaan,
      table: table,
      dlpd: dlpd_hitung,
      unitup: params.unitup,
      blth_filter: params.blth
    });

    if (!table || !blth || !kdkelompok) {
      alert("❌ Parameter table, BLTH, dan KDKELOMPOK wajib ada");
      return;
    }

    $("#detailModal").attr("data-table", table);
    $("#detailTable").attr("data-table", table);
    $("#detailModalLabel").text(
      `Detail Pelanggan untuk ${blth} - ${kdkelompok} (Status: ${hasil_pemeriksaan || "BELUM DIISI"})`
    );
    $("#detailTableHead, #detailTableBody").empty();

    $.ajax({
      url: "/monitoring/get_detail_pelanggan_koreksi",
      method: "GET",
      data: {
        blth: params.blth || blth,  // ✅ Prioritaskan blth dari filter
        kdkelompok: kdkelompok,
        hasil_pemeriksaan: hasil_pemeriksaan,
        table: table,
        dlpd_hitung: dlpd_hitung,
        unitup: params.unitup  // ✅ ADDED
      },
      success: function (response) {
        console.log("✅ Data received:", response);
        renderModalTable(response.data, "koreksi");
      },
      error: function (xhr) {
        console.error("❌ AJAX Error:", xhr.responseText);
        const errorMsg = xhr.responseJSON?.error || xhr.statusText || "Gagal akses database";
        alert("❌ " + errorMsg);
        $("#detailTableBody").html(
          '<tr><td colspan="18" class="text-center text-danger">Gagal memuat data: ' +
            errorMsg +
            "</td></tr>"
        );
        const modal = new bootstrap.Modal(document.getElementById("detailModal"));
        modal.show();
      },
    });
  });

  // ================================================================
  // HANDLER 4: Detail Link (Default - Hari Baca)
  // ================================================================
  $(document).on("click", ".detail-link", function (e) {
    e.preventDefault();

    const params = getUrlParams();
    const blth = $(this).data("blth");
    const kdkelompok = $(this).data("kdkelompok");
    const hasil_pemeriksaan = $(this).data("hasilpemeriksaan");
    const table = $(this).data("table");
    const dlpd_hitung = $(this).data("dlpd");

    console.log("🔍 Link DEFAULT clicked:", {
      blth: blth,
      kdkelompok: kdkelompok,
      hasil: hasil_pemeriksaan,
      table: table,
      dlpd: dlpd_hitung,
      unitup: params.unitup,
      blth_filter: params.blth
    });

    $("#detailModal").attr("data-table", table);
    $("#detailTable").attr("data-table", table);
    $("#detailModalLabel").text(
      `Detail Pelanggan untuk ${blth} - ${kdkelompok} (Status: ${hasil_pemeriksaan || "BELUM DIISI"})`
    );
    $("#detailTableHead, #detailTableBody").empty();

    $.ajax({
      url: "/monitoring/get_detail_pelanggan_dlpd_hb",
      method: "GET",
      data: {
        blth: params.blth || blth,  // ✅ Prioritaskan blth dari filter
        kdkelompok: kdkelompok,
        hasil_pemeriksaan: hasil_pemeriksaan,
        table: table,
        dlpd_hitung: dlpd_hitung,
        unitup: params.unitup  // ✅ ADDED
      },
      success: function (response) {
        console.log("✅ Data received:", response);
        renderModalTable(response.data, "default");
      },
      error: function (xhr) {
        console.error("❌ AJAX Error:", xhr.responseText);
        const errorMsg = xhr.responseJSON?.error || xhr.statusText || "Gagal akses database";
        alert("❌ " + errorMsg);
        $("#detailTableBody").html(
          '<tr><td colspan="18" class="text-center text-danger">Gagal memuat data: ' +
            errorMsg +
            "</td></tr>"
        );
        const modal = new bootstrap.Modal(document.getElementById("detailModal"));
        modal.show();
      },
    });
  });

  // ================================================================
  // FUNCTION: Render Modal Table
  // ================================================================
  function renderModalTable(data, type) {
    const hasilOptions = [
      "SESUAI",
      "TEMPER NYALA",
      "SALAH STAN",
      "SALAH FOTO",
      "FOTO BURAM",
      "ANOMALI PDL",
      "LEBIH TAGIH",
      "KURANG TAGIH",
      "BKN FOTO KWH",
      "BENCANA",
      "3BLN TANPA STAN",
      "BACA ULANG",
      "MASUK 720JN",
    ];

    const baseColumns = [
      "BLTH",
      "UNITUP",
      "IDPEL",
      "NAMA",
      "TARIF",
      "DAYA",
      "SAHLWBP",
      "DELTA_PEMKWH",
      "PERSEN",
      "JAM_NYALA",
      "DLPD_HITUNG",
      "FOTO_3BLN",
      "HASIL_PEMERIKSAAN",
      "STAN_VERIFIKASI",
      "TINDAK_LANJUT",
      "KET",
    ];

    const visibleColumns = [];
    baseColumns.forEach((col) => {
      if (col === "HASIL_PEMERIKSAAN") {
        if (type === "ganda") {
          visibleColumns.push("VERIFIKASI");
        } else {
          visibleColumns.push("FOTO_3BLN_AP2T");
        }
      }
      visibleColumns.push(col);
    });

    let originalData = data;
    let currentSort = { column: null, direction: "asc" };

    function renderTableHeader() {
      $("#detailTableHead").empty();
      let headerRow = "<tr><th>No</th>";
      visibleColumns.forEach(function (col) {
        const displayName = col.replace(/_/g, " ");
        headerRow += `<th class="sortable-header" data-column="${col}"> ${displayName} <span class="sort-icon float-end"> ${
          currentSort.column === col
            ? currentSort.direction === "asc"
              ? '<i class="bi bi-sort-up"></i>'
              : '<i class="bi bi-sort-down"></i>'
            : '<i class="bi bi-arrow-down-up" style="opacity: 0.3;"></i>'
        } </span> </th>`;
      });
      headerRow += "<th>DATA DETAIL</th></tr>";
      $("#detailTableHead").append(headerRow);
    }

    function renderTableBody(data) {
      $("#detailTableBody").empty();
      data.forEach(function (row, index) {
        let tr = `<tr><td>${index + 1}</td>`;
        visibleColumns.forEach(function (col) {
          if (col === "HASIL_PEMERIKSAAN") {
            tr +=
              '<td><select name="hasil_' +
              row["IDPEL"] +
              '" class="form-select form-select-sm">';
            tr += '<option value="">-</option>';
            hasilOptions.forEach(function (opt) {
              const selected = row[col] === opt ? "selected" : "";
              tr += `<option value="${opt}" ${selected}>${opt}</option>`;
            });
            tr += "</select></td>";
          } else if (col === "STAN_VERIFIKASI") {
            const val = row[col] || "";
            tr += `<td><input type="text" class="form-control form-control-sm stan-verifikasi-input" data-idpel="${row["IDPEL"]}" value="${val}"></td>`;
          } else if (col === "TINDAK_LANJUT") {
            let val = row[col] || "";
            tr += `<td><textarea name="tindak_${row["IDPEL"]}" rows="2" class="form-control form-control-sm">${val}</textarea></td>`;
          } else if (col === "VERIFIKASI") {
            tr += `<td> <button type="button" class="btn btn-sm btn-info foto-3bln-ap2t-btn" data-idpel="${
              row["IDPEL"]
            }" data-sahlwbp="${
              row["SAHLWBP"] || ""
            }"> <i class="fas fa-check-circle"></i> VERIFIED </button> </td>`;
          } else if (col === "FOTO_3BLN_AP2T") {
            tr += `<td> <button type="button" class="btn btn-sm btn-warning foto-3bln-ap2t-btn" data-idpel="${row["IDPEL"]}" data-sahlwbp="${row["SAHLWBP"]}" onclick="open3FotoAP2T('${row["IDPEL"]}', '${row["BLTH"]}')"> <i class="fas fa-image"></i> Link AP2T </button> </td>`;
          } else if (col === "FOTO_3BLN") {
            let fotoHtml = row[col] || "";
            fotoHtml = fotoHtml.replace(
              /<a\b([^>]*)>/,
              `<a class="foto-3bln-link" data-idpel="${row["IDPEL"]}" data-sahlwbp="${row["SAHLWBP"]}" $1>`
            );
            tr += `<td>${fotoHtml}</td>`;
          } else {
            tr += `<td>${
              row[col] !== null && row[col] !== undefined ? row[col] : ""
            }</td>`;
          }
        });
        tr += `<td><button class="btn btn-sm btn-info detail-plg-btn" data-idpel="${row["IDPEL"]}"> <i class="fas fa-info-circle"></i> Detail </button></td></tr>`;
        $("#detailTableBody").append(tr);
      });
    }

    renderTableHeader();
    renderTableBody(originalData);

    // Sorting functionality
    $(document).off("click", ".sortable-header");
    $(document).on("click", ".sortable-header", function () {
      const columnName = $(this).data("column");
      if (currentSort.column === columnName) {
        currentSort.direction = currentSort.direction === "asc" ? "desc" : "asc";
      } else {
        currentSort.column = columnName;
        currentSort.direction = "asc";
      }

      const sortedData = [...originalData].sort((a, b) => {
        const valA = a[columnName];
        const valB = b[columnName];
        if (valA === valB) return 0;
        if (valA === null || valA === undefined) return 1;
        if (valB === null || valB === undefined) return -1;
        if (!isNaN(valA) && !isNaN(valB)) {
          return currentSort.direction === "asc"
            ? parseFloat(valA) - parseFloat(valB)
            : parseFloat(valB) - parseFloat(valA);
        }
        return currentSort.direction === "asc"
          ? String(valA).localeCompare(String(valB))
          : String(valB).localeCompare(String(valA));
      });

      renderTableHeader();
      renderTableBody(sortedData);
      typeof highlightModalTable === "function" && highlightModalTable();
    });

    typeof highlightModalTable === "function" && highlightModalTable();
    const modal = new bootstrap.Modal(document.getElementById("detailModal"));
    modal.show();
  }

  // ================================================================
  // FUNCTION: Update Stan Verifikasi dan Hasil Pemeriksaan
  // ================================================================
  function updateStanVerifikasiDanHasil(idpel, sahlwbp) {
    const inputEl = $(`input.stan-verifikasi-input[data-idpel="${idpel}"]`);
    if (inputEl.length) {
      const currentVal = (inputEl.val() || "").trim();
      if (currentVal === "" || currentVal === "-") {
        inputEl.val(sahlwbp).addClass("is-valid");
        console.log(`STAN_VERIFIKASI diubah ke: ${sahlwbp}`);
      } else {
        console.log(
          `STAN_VERIFIKASI sudah terisi: ${currentVal}, tidak diubah.`
        );
      }
    }

    const selectEl = $(`select[name="hasil_${idpel}"]`);
    if (selectEl.length) {
      const currentVal = selectEl.val().trim();
      if (currentVal === "" || currentVal === "-") {
        if (selectEl.find('option[value="SESUAI"]').length === 0) {
          selectEl.append('<option value="SESUAI">SESUAI</option>');
        }
        selectEl.val("SESUAI").trigger("change").addClass("is-valid");
        console.log(`HASIL_PEMERIKSAAN diubah ke: SESUAI`);
      } else {
        console.log(
          `HASIL_PEMERIKSAAN sudah diisi: ${currentVal}, tidak diubah.`
        );
      }
    }
  }

  // ================================================================
  // EVENT: Click pada foto link dan button
  // ================================================================
  $(document).on("click", ".foto-3bln-link, .foto-3bln-ap2t-btn", function (e) {
    e.preventDefault();
    const idpel = $(this).data("idpel");
    const sahlwbp = ($(this).data("sahlwbp") || "").toString();
    updateStanVerifikasiDanHasil(idpel, sahlwbp);
  });

  // ================================================================
  // EVENT: Detail Pelanggan Button
  // ================================================================
  function getBlthFromUrlOrSelect() {
  try {
    const url = new URL(window.location.href);
    const q = url.searchParams.get('blth');
    if (q) return q;
  } catch (err) {
    // ignore invalid URL
  }
  const sel = document.querySelector('#blthFilter');
  if (sel) return sel.value;
  return '';
}

  $(document).on("click", ".detail-plg-btn", function () {
    const button = $(this);
    const idpel = button.data("idpel");
    const table = $("#detailModal").attr("data-table");

    if (!table) {
      alert("❌ Table tidak ditemukan. Silakan buka ulang modal induk.");
      return;
    }

    if (button.hasClass("loading")) return;
    button.addClass("loading").prop("disabled", true);

    const detailModalEl = document.createElement("div");
    detailModalEl.className = "modal fade";
    detailModalEl.innerHTML = `
      <div class="modal-dialog modal-lg modal-dialog-scrollable">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title">Loading...</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
          </div>
          <div class="modal-body text-center py-4">
            <div class="spinner-border text-primary" role="status">
              <span class="visually-hidden">Loading...</span>
            </div>
            <p class="mt-2">Memuat detail pelanggan...</p>
          </div>
        </div>
      </div>`;
    document.body.appendChild(detailModalEl);

    const detailModal = new bootstrap.Modal(detailModalEl);
    detailModal.show();

    const cleanup = () => {
      button.removeClass("loading").prop("disabled", false);
      if (document.body.contains(detailModalEl)) {
        document.body.removeChild(detailModalEl);
      }
    };

    const fetchTimer = setTimeout(() => {
      if (
        !detailModalEl
          .querySelector(".modal-body")
          .innerHTML.includes("alert")
      ) {
        detailModalEl.querySelector(".modal-body").innerHTML = `
          <div class="alert alert-warning">
            <i class="fas fa-exclamation-triangle"></i> Request timeout
          </div>`;
      }
    }, 60000);
    const blthParam = getBlthFromUrlOrSelect();

    $.ajax({
      url: "/monitoring/get_full_customer_detail",
      method: "GET",
      // data: { idpel: idpel, table: table },
      data: { idpel: idpel, table: table, blth: blthParam },

      success: function (response) {
        clearTimeout(fetchTimer);

        if (!response.data) {
          detailModalEl.querySelector(".modal-body").innerHTML = `
            <div class="alert alert-warning">
              <i class="fas fa-info-circle"></i> Data pelanggan tidak ditemukan.
            </div>`;
          return;
        }

        let detailHtml = `
          <div class="container-fluid">
            <h5 class="mb-3">Detail Pelanggan ${idpel}</h5>
            <div class="table-responsive">
              <table class="table table-sm table-bordered table-striped">
                <tbody>`;

        let grafikUrl = null;
        const data = response.data;

        const orderedKeys = [
          "BLTH",
          "UNITUP",
          "IDPEL",
          "NAMA",
          "TARIF",
          "DAYA",
          "SLALWBP",
          "LWBPCABUT",
          "SELISIH_STAN_BONGKAR",
          "LWBPPASANG",
          "KWH_SEKARANG",
          "KWH_1_BULAN_LALU",
          "KWH_2_BULAN_LALU",
          "SAHLWBP",
          "DELTA_PEMKWH",
          "PERSEN",
          "JAM_NYALA",
          "JAMNYALA600",
          "NOMORKWH",
          "GRAFIK",
          "FOTO_AKHIR",
          "FOTO_LALU",
          "FOTO_LALU2",
          "FOTO_3BLN",
          "HASIL_PEMERIKSAAN",
          "STAN_VERIFIKASI",
          "TINDAK_LANJUT",
          "KET",
          "KDKELOMPOK",
          "DLPD",
          "DLPD_3BLN",
          "DLPD_HITUNG",
          "MARKING_KOREKSI",
        ];

        orderedKeys.forEach((key) => {
          const value = data[key];
          if (value === null || value === "") return;

          let displayValue = value;
          if (
            key === "FOTO_3BLN" ||
            key === "FOTO_AKHIR" ||
            key === "FOTO_LALU" ||
            key === "FOTO_LALU2"
          ) {
            displayValue = value.includes("http")
              ? `<a href="${value}" target="_blank" class="btn btn-sm btn-outline-primary">View ${key.replace(
                  /_/g,
                  " "
                )}</a>`
              : value;
          }

          detailHtml += `
            <tr>
              <th style="width:30%" class="text-nowrap text-start">${key.replace(
                /_/g,
                " "
              )}</th>
              <td class="text-start">${displayValue}</td>
            </tr>`;
        });

        const blth = data["BLTH"] || "";
        let ulp = "";
        if (table.includes("billing_")) {
          ulp = table
            .replace("billing_", "")
            .replace(/[0-9]/g, "")
            .toLowerCase();
        }

        if (blth && ulp) {
          grafikUrl = `${window.location.origin}/grafik/${idpel}?blth=${blth}&ulp=${ulp}`;
        }

        detailHtml += `</tbody></table></div>`;

        if (grafikUrl) {
          detailHtml += `
            <div class="mt-4">
              <h6>Grafik Pemakaian</h6>
              <iframe src="${grafikUrl}" width="100%" height="400" style="border:1px solid #ccc; border-radius:8px;"></iframe>
            </div>`;
        }

        detailModalEl.querySelector(
          ".modal-title"
        ).innerText = `Detail Pelanggan ${idpel}`;
        detailModalEl.querySelector(".modal-body").innerHTML = detailHtml;
      },
      error: function (xhr) {
        clearTimeout(fetchTimer);
        detailModalEl.querySelector(".modal-body").innerHTML = `
          <div class="alert alert-danger">
            <i class="fas fa-exclamation-triangle"></i> Gagal memuat detail pelanggan: ${
              xhr.responseJSON?.error || xhr.statusText
            }
          </div>`;
      },
    });

    detailModalEl.addEventListener("hidden.bs.modal", function () {
      cleanup();
    });

    const escapeHandler = function (e) {
      if (e.key === "Escape") {
        cleanup();
        document.removeEventListener("keydown", escapeHandler);
      }
    };
    document.addEventListener("keydown", escapeHandler);
  });

  // ================================================================
  // EVENT: Save Changes Button
  // ================================================================
  $("#saveChangesBtn").on("click", function () {
    const btn = $(this);
    const params = getUrlParams();
    let updates = [];
    let tableName = $("#detailModal").attr("data-table");

    console.log("🔄 Preparing to save changes...");
    console.log("📋 Table:", tableName);

    $("#detailTableBody tr").each(function () {
      let row = $(this);
      
      // Get IDPEL from different possible sources
      let idpel = row.find("input.stan-verifikasi-input").data("idpel");
      if (!idpel) {
        let hasilSelect = row.find("select[name^='hasil_']");
        if (hasilSelect.length) {
          idpel = hasilSelect.attr("name").split("_")[1];
        }
      }
      
      // Skip if no IDPEL found
      if (!idpel) {
        console.warn("⚠️ No IDPEL found for row, skipping");
        return;
      }

      let hasil = row.find(`select[name="hasil_${idpel}"]`).val() || "";
      let tindak = row.find(`textarea[name="tindak_${idpel}"]`).val() || "";
      let stan_verifikasi = row.find(`input.stan-verifikasi-input[data-idpel="${idpel}"]`).val() || "";

      updates.push({
        IDPEL: idpel,
        HASIL: hasil,
        TINDAK: tindak,
        STAN: stan_verifikasi
      });
    });

    console.log(`📊 Total updates: ${updates.length}`);
    console.log("📦 Updates data:", updates);

    if (updates.length === 0) {
      alert("⚠️ Tidak ada data untuk disimpan");
      return;
    }

    btn
      .prop("disabled", true)
      .html(
        '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Menyimpan...'
      );

    const requestData = {
      table: tableName,
      updates: updates,
      unitup: params.unitup,
      blth: params.blth
    };

    console.log("🚀 Sending request:", requestData);

    $.ajax({
      url: "/monitoring/update_hasil_pemeriksaan",
      method: "POST",
      contentType: "application/json",
      data: JSON.stringify(requestData),
      success: function (response) {
        console.log("✅ Save successful:", response);
        alert(`✅ ${response.message || 'Data berhasil disimpan!'}`);
        location.reload();
      },
      error: function (xhr, status, error) {
        console.error("❌ Save failed:", {
          status: xhr.status,
          statusText: xhr.statusText,
          response: xhr.responseText,
          error: error
        });
        
        let errorMsg = "Gagal menyimpan data.";
        if (xhr.responseJSON && xhr.responseJSON.message) {
          errorMsg = xhr.responseJSON.message;
        } else if (xhr.responseText) {
          try {
            const errData = JSON.parse(xhr.responseText);
            errorMsg = errData.message || errorMsg;
          } catch (e) {
            errorMsg = xhr.responseText.substring(0, 100);
          }
        }
        
        alert(`❌ ${errorMsg}`);
        btn.prop("disabled", false).html("Simpan Perubahan");
      },
    });
  });
});