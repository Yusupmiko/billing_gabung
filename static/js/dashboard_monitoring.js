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

    // $("#detailModal").attr("data-table", table);
 $("#detailModal").attr("data-table", table);
  $("#detailModal").attr("data-blth", params.blth);      // ✅ ADDED
  $("#detailModal").attr("data-unitup", params.unitup);  // ✅ ADDED

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
        unitup: params.unitup,
        blth: params.blth
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
  $("#detailModal").attr("data-blth", params.blth);      // ✅ ADDED
  $("#detailModal").attr("data-unitup", params.unitup);  // ✅ ADDED
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
        unitup: params.unitup,
        blth: params.blth
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
  $("#detailModal").attr("data-blth", params.blth);      // ✅ ADDED
  $("#detailModal").attr("data-unitup", params.unitup);  // ✅ ADDED
    $("#detailModalLabel").text(
      `Detail Pelanggan untuk ${blth} - ${kdkelompok} (Status: ${hasil_pemeriksaan || "BELUM DIISI"})`
    );
    $("#detailTableHead, #detailTableBody").empty();

    $.ajax({
      url: "/monitoring/get_detail_pelanggan_koreksi",
      method: "GET",
      data: {
        blth: params.blth || blth,
        kdkelompok: kdkelompok,
        hasil_pemeriksaan: hasil_pemeriksaan,
        table: table,
        dlpd_hitung: dlpd_hitung,
        unitup: params.unitup
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
  
  // ✅ SIMPAN BLTH dan UNITUP ke modal
  $("#detailModal").attr("data-blth", params.blth || blth);  // ✅ ADDED
  $("#detailModal").attr("data-unitup", params.unitup);      // ✅ ADDED
  
  $("#detailModalLabel").text(
    `Detail Pelanggan untuk ${blth} - ${kdkelompok} (Status: ${hasil_pemeriksaan || "BELUM DIISI"})`
  );
  $("#detailTableHead, #detailTableBody").empty();

  $.ajax({
    url: "/monitoring/get_detail_pelanggan_dlpd_hb",
    method: "GET",
    data: {
      blth: params.blth || blth,
      kdkelompok: kdkelompok,
      hasil_pemeriksaan: hasil_pemeriksaan,
      table: table,
      dlpd_hitung: dlpd_hitung,
      unitup: params.unitup
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
  // function renderModalTable(data, type) {
  //   const hasilOptions = [
  //     "SESUAI",
  //     "TEMPER NYALA",
  //     "SALAH STAN",
  //     "SALAH FOTO",
  //     "FOTO BURAM",
  //     "ANOMALI PDL",
  //     "LEBIH TAGIH",
  //     "KURANG TAGIH",
  //     "BKN FOTO KWH",
  //     "BENCANA",
  //     "3BLN TANPA STAN",
  //     "BACA ULANG",
  //     "MASUK 720JN",
  //   ];

  //   const baseColumns = [
  //     "BLTH",
  //     "UNITUP",
  //     "IDPEL",
  //     "NAMA",
  //     "TARIF",
  //     "DAYA",
  //     "SAHLWBP",
  //     "DELTA_PEMKWH",
  //     "PERSEN",
  //     "JAM_NYALA",
  //     "DLPD_HITUNG",
  //     "FOTO_3BLN",
  //     "HASIL_PEMERIKSAAN",
  //     "STAN_VERIFIKASI",
  //     "TINDAK_LANJUT",
  //     "KET",
  //   ];

  //   const visibleColumns = [];
  //   baseColumns.forEach((col) => {
  //     if (col === "HASIL_PEMERIKSAAN") {
  //       if (type === "ganda") {
  //         visibleColumns.push("VERIFIKASI");
  //       } else {
  //         visibleColumns.push("FOTO_3BLN_AP2T");
  //       }
  //     }
  //     visibleColumns.push(col);
  //   });

  //   let originalData = data;
  //   let currentSort = { column: null, direction: "asc" };

  //   function renderTableHeader() {
  //     $("#detailTableHead").empty();
  //     let headerRow = "<tr><th>No</th>";
  //     visibleColumns.forEach(function (col) {
  //       const displayName = col.replace(/_/g, " ");
  //       headerRow += `<th class="sortable-header" data-column="${col}"> ${displayName} <span class="sort-icon float-end"> ${
  //         currentSort.column === col
  //           ? currentSort.direction === "asc"
  //             ? '<i class="bi bi-sort-up"></i>'
  //             : '<i class="bi bi-sort-down"></i>'
  //           : '<i class="bi bi-arrow-down-up" style="opacity: 0.3;"></i>'
  //       } </span> </th>`;
  //     });
  //     headerRow += "<th>DATA DETAIL</th></tr>";
  //     $("#detailTableHead").append(headerRow);
  //   }

  //   function renderTableBody(data) {
  //     $("#detailTableBody").empty();
  //     data.forEach(function (row, index) {
  //       let tr = `<tr><td>${index + 1}</td>`;
  //       visibleColumns.forEach(function (col) {
  //         if (col === "HASIL_PEMERIKSAAN") {
  //           tr +=
  //             '<td><select name="hasil_' +
  //             row["IDPEL"] +
  //             '" class="form-select form-select-sm">';
  //           tr += '<option value="">-</option>';
  //           hasilOptions.forEach(function (opt) {
  //             const selected = row[col] === opt ? "selected" : "";
  //             tr += `<option value="${opt}" ${selected}>${opt}</option>`;
  //           });
  //           tr += "</select></td>";
  //         } else if (col === "STAN_VERIFIKASI") {
  //           const val = row[col] || "";
  //           tr += `<td><input type="text" class="form-control form-control-sm stan-verifikasi-input" data-idpel="${row["IDPEL"]}" value="${val}"></td>`;
  //         } else if (col === "TINDAK_LANJUT") {
  //           let val = row[col] || "";
  //           tr += `<td><textarea name="tindak_${row["IDPEL"]}" rows="2" class="form-control form-control-sm">${val}</textarea></td>`;
  //         } else if (col === "VERIFIKASI") {
  //           tr += `<td> <button type="button" class="btn btn-sm btn-info foto-3bln-ap2t-btn" data-idpel="${
  //             row["IDPEL"]
  //           }" data-sahlwbp="${
  //             row["SAHLWBP"] || ""
  //           }"> <i class="fas fa-check-circle"></i> VERIFIED </button> </td>`;
  //         } else if (col === "FOTO_3BLN_AP2T") {
  //           tr += `<td> <button type="button" class="btn btn-sm btn-warning foto-3bln-ap2t-btn" data-idpel="${row["IDPEL"]}" data-sahlwbp="${row["SAHLWBP"]}" onclick="open3FotoAP2T('${row["IDPEL"]}', '${row["BLTH"]}')"> <i class="fas fa-image"></i> Link AP2T </button> </td>`;
  //         } else if (col === "FOTO_3BLN") {
  //           let fotoHtml = row[col] || "";
  //           fotoHtml = fotoHtml.replace(
  //             /<a\b([^>]*)>/,
  //             `<a class="foto-3bln-link" data-idpel="${row["IDPEL"]}" data-sahlwbp="${row["SAHLWBP"]}" $1>`
  //           );
  //           tr += `<td>${fotoHtml}</td>`;
  //         } else {
  //           tr += `<td>${
  //             row[col] !== null && row[col] !== undefined ? row[col] : ""
  //           }</td>`;
  //         }
  //       });
  //       tr += `<td><button class="btn btn-sm btn-info detail-plg-btn" data-idpel="${row["IDPEL"]}" data-blth="${row["BLTH"]}"> <i class="fas fa-info-circle"></i> Detail </button></td></tr>`;
  //       $("#detailTableBody").append(tr);
  //     });
  //   }

  //   renderTableHeader();
  //   renderTableBody(originalData);

  //   // Sorting functionality
  //   $(document).off("click", ".sortable-header");
  //   $(document).on("click", ".sortable-header", function () {
  //     const columnName = $(this).data("column");
  //     if (currentSort.column === columnName) {
  //       currentSort.direction = currentSort.direction === "asc" ? "desc" : "asc";
  //     } else {
  //       currentSort.column = columnName;
  //       currentSort.direction = "asc";
  //     }

  //     const sortedData = [...originalData].sort((a, b) => {
  //       const valA = a[columnName];
  //       const valB = b[columnName];
  //       if (valA === valB) return 0;
  //       if (valA === null || valA === undefined) return 1;
  //       if (valB === null || valB === undefined) return -1;
  //       if (!isNaN(valA) && !isNaN(valB)) {
  //         return currentSort.direction === "asc"
  //           ? parseFloat(valA) - parseFloat(valB)
  //           : parseFloat(valB) - parseFloat(valA);
  //       }
  //       return currentSort.direction === "asc"
  //         ? String(valA).localeCompare(String(valB))
  //         : String(valB).localeCompare(String(valA));
  //     });

  //     renderTableHeader();
  //     renderTableBody(sortedData);
  //     typeof highlightModalTable === "function" && highlightModalTable();
  //   });

  //   typeof highlightModalTable === "function" && highlightModalTable();
  //   const modal = new bootstrap.Modal(document.getElementById("detailModal"));
  //   modal.show();
  // }
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
    // "UNITUP",
    "IDPEL",
    "NAMA",
    // "TARIF",
    // "DAYA",
    "DELTA_PEMKWH",
    "PERSEN",
    // "JAM_NYALA",
    "DLPD_HITUNG",
    // "GRAFIK KWH",
    "FOTO_3BLN",
    "NOMORKWH",
    "SAHLWBP",
    "HASIL_PEMERIKSAAN",
    "STAN_VERIFIKASI",
    "TINDAK_LANJUT",
    // "KET",
  ];

  const visibleColumns = [];
  baseColumns.forEach((col) => {
    if (col === "HASIL_PEMERIKSAAN") {
      if (type === "ganda") {
        visibleColumns.push("VERIFIKASI");
      } else {
        // visibleColumns.push("FOTO_3BLN_AP2T");
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
      let tr = `<tr data-idpel="${row["IDPEL"]}"><td>${index + 1}</td>`;
      visibleColumns.forEach(function (col) {
        if (col === "HASIL_PEMERIKSAAN") {
          const originalValue = row[col] || "";
          tr += `<td><select name="hasil_${row["IDPEL"]}" class="form-select form-select-sm" data-original="${originalValue}">`;
          tr += '<option value="">-</option>';
          hasilOptions.forEach(function (opt) {
            const selected = row[col] === opt ? "selected" : "";
            tr += `<option value="${opt}" ${selected}>${opt}</option>`;
          });
          tr += "</select></td>";
        } else if (col === "STAN_VERIFIKASI") {
          const val = row[col] || "";
          tr += `<td><input type="text" class="form-control form-control-sm stan-verifikasi-input" data-idpel="${row["IDPEL"]}" data-original="${val}" value="${val}"></td>`;
        } else if (col === "TINDAK_LANJUT") {
          let val = row[col] || "";
          tr += `<td><textarea name="tindak_${row["IDPEL"]}" rows="2" class="form-control form-control-sm" data-original="${val}">${val}</textarea></td>`;
        } else if (col === "VERIFIKASI") {
          tr += `<td> <button type="button" class="btn btn-sm btn-info foto-3bln-ap2t-btn" data-idpel="${
            row["IDPEL"]
          }" data-sahlwbp="${
            row["SAHLWBP"] || ""
          }"> <i class="fas fa-check-circle"></i> VERIFIED </button> </td>`;
        } 
        
        
        else if (col === "FOTO_3BLN") {
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
      tr += `<td><button class="btn btn-sm btn-info detail-plg-btn" data-idpel="${row["IDPEL"]}" data-blth="${row["BLTH"]}"> <i class="fas fa-info-circle"></i> Detail </button></td></tr>`;
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
      let valA = a[columnName];
      let valB = b[columnName];
      
      if (valA === valB) return 0;
      if (valA === null || valA === undefined || valA === "") return 1;
      if (valB === null || valB === undefined || valB === "") return -1;
      
      // ✅ Bersihkan nilai numerik (hapus %, koma, dan karakter non-numeric)
      const cleanA = String(valA).replace(/[%,]/g, '').trim();
      const cleanB = String(valB).replace(/[%,]/g, '').trim();
      
      const numA = parseFloat(cleanA);
      const numB = parseFloat(cleanB);
      
      // ✅ Jika keduanya adalah angka valid, sort secara numerik
      if (!isNaN(numA) && !isNaN(numB)) {
        return currentSort.direction === "asc"
          ? numA - numB
          : numB - numA;
      }
      
      // ✅ Jika bukan angka, sort sebagai string
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
  // EVENT: Detail Pelanggan Button - ✅ WITH GRAFIK
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
    const blth = button.data("blth");  // ✅ AMBIL BLTH dari button
    const table = $("#detailModal").attr("data-table");
    const params = getUrlParams();

    console.log("🔍 Detail button clicked:", {
      idpel: idpel,
      blth: blth,
      table: table,
      unitup: params.unitup
    });

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

      data: { 
    idpel: idpel, 
    table: table,
    unitup: params.unitup,
    blth: blthParam
},


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
          // "GRAFIK",
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
        
        // ✅ PERBAIKAN: Khusus untuk kolom FOTO, render sebagai HTML
        if (
          key === "FOTO_3BLN" ||
          key === "FOTO_AKHIR" ||
          key === "FOTO_LALU" ||
          key === "FOTO_LALU2"
        ) {
          // Cek apakah value mengandung HTML button
          if (value.includes("<button")) {
            displayValue = value;  // ✅ Langsung pakai HTML button
          } else if (value.includes("http")) {
            // Fallback jika hanya URL
            displayValue = `<a href="${value}" target="_blank" class="btn btn-sm btn-outline-primary">
              <i class="bi bi-image"></i> View ${key.replace(/_/g, " ")}
            </a>`;
          } else {
            displayValue = value;
          }
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

        detailHtml += `</tbody></table></div>`;

        // ========================================
        // ✅ GRAFIK SECTION - MENGGUNAKAN UNITUP
        // ========================================
        const blthGrafik = data["BLTH"] || blth || params.blth;
        const unitupGrafik = data["UNITUP"] || params.unitup;

        console.log("📊 Grafik params:", {
          idpel: idpel,
          blth: blthGrafik,
          unitup: unitupGrafik
        });

        if (blthGrafik && unitupGrafik) {
          grafikUrl = `${window.location.origin}/grafik/${idpel}?blth=${blthGrafik}&ulp=${unitupGrafik}`;
          
          detailHtml += `
            <div class="mt-4">
              <h6>📈 Grafik Pemakaian 6 Bulan Terakhir</h6>
              <iframe 
                src="${grafikUrl}" 
                width="100%" 
                height="400" 
                style="border:1px solid #ccc; border-radius:8px;"
                onload="console.log('✅ Grafik loaded successfully')"
                onerror="console.error('❌ Grafik failed to load')">
              </iframe>
            </div>`;
        } else {
          console.warn("⚠️ Grafik tidak ditampilkan. Missing params:", {
            blth: blthGrafik,
            unitup: unitupGrafik
          });
        }
        // ========================================

        detailModalEl.querySelector(
          ".modal-title"
        ).innerText = `Detail Pelanggan ${idpel}`;
        detailModalEl.querySelector(".modal-body").innerHTML = detailHtml;
      },
      error: function (xhr) {
        clearTimeout(fetchTimer);
        console.error("❌ AJAX Error:", xhr);
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
// SAVE CHANGES: KIRIM HANYA DATA YANG BERUBAH
// ================================================================
// ================================================================
// SAVE CHANGES: KIRIM HANYA DATA YANG BERUBAH
// ================================================================
$("#saveChangesBtn").on("click", function () {
  const btn = $(this);
  let updates = [];
  let tableName = $("#detailModal").attr("data-table");
  
  let blth = $("#detailModal").attr("data-blth");
  let unitup = $("#detailModal").attr("data-unitup");

  // ✅ FALLBACK 1: Ambil dari URL
  if (!unitup || unitup.trim() === "") {
    const urlParams = new URLSearchParams(window.location.search);
    unitup = urlParams.get('unitup') || '';
    console.log("⚠️ Using UNITUP from URL:", unitup);
  }

  // ✅ FALLBACK 2: Ambil dari row pertama
  if (!unitup || unitup.trim() === "") {
    const firstRow = $("#detailTableBody tr:first");
    if (firstRow.length) {
      unitup = firstRow.find("td").eq(1).text().trim();
      console.log("⚠️ Using UNITUP from table:", unitup);
    }
  }

  console.log("🔄 Checking for changes...");
  console.log("📋 Table:", tableName);
  console.log("📅 BLTH:", blth);
  console.log("🏢 UNITUP:", unitup);

  // ✅ VALIDASI
  if (!blth || blth.trim() === "") {
    alert("⚠️ BLTH tidak ditemukan! Silakan buka ulang modal.");
    return;
  }

  if (!unitup || unitup.trim() === "") {
    alert("⚠️ UNITUP tidak ditemukan! Silakan buka ulang modal.");
    return;
  }

  // ✅ LOOP: Cari hanya row yang BERUBAH
  $("#detailTableBody tr").each(function () {
    let row = $(this);
    let idpel = row.data("idpel");
    
    if (!idpel) {
      console.warn("⚠️ No IDPEL found for row, skipping");
      return;
    }

    // Cek HASIL_PEMERIKSAAN
    const hasilSelect = row.find(`select[name="hasil_${idpel}"]`);
    const hasilOriginal = hasilSelect.data("original") || "";
    const hasilCurrent = hasilSelect.val() || "";
    const hasilChanged = hasilOriginal !== hasilCurrent;

    // Cek TINDAK_LANJUT
    const tindakTextarea = row.find(`textarea[name="tindak_${idpel}"]`);
    const tindakOriginal = tindakTextarea.data("original") || "";
    const tindakCurrent = tindakTextarea.val() || "";
    const tindakChanged = tindakOriginal !== tindakCurrent;

    // Cek STAN_VERIFIKASI
    const stanInput = row.find(`input.stan-verifikasi-input[data-idpel="${idpel}"]`);
    const stanOriginal = stanInput.data("original") || "";
    const stanCurrent = stanInput.val() || "";
    const stanChanged = stanOriginal !== stanCurrent;

    // ✅ HANYA TAMBAHKAN JIKA ADA PERUBAHAN
    if (hasilChanged || tindakChanged || stanChanged) {
      console.log(`✏️ CHANGED: IDPEL ${idpel}`, {
        hasil: hasilChanged ? `"${hasilOriginal}" → "${hasilCurrent}"` : "no change",
        tindak: tindakChanged ? `"${tindakOriginal}" → "${tindakCurrent}"` : "no change",
        stan: stanChanged ? `"${stanOriginal}" → "${stanCurrent}"` : "no change"
      });

      updates.push({
        IDPEL: idpel,
        BLTH: blth,
        UNITUP: unitup,
        HASIL: hasilCurrent,
        TINDAK: tindakCurrent,
        STAN: stanCurrent
      });
    }
  });

  console.log(`📊 Total changes detected: ${updates.length} out of ${$("#detailTableBody tr").length} rows`);

  if (updates.length === 0) {
    alert("ℹ️ Tidak ada perubahan untuk disimpan");
    return;
  }

  btn
    .prop("disabled", true)
    .html(
      '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Menyimpan...'
    );

  const requestData = {
    table: tableName,
    updates: updates
  };

  console.log("🚀 Sending request:", requestData);

  $.ajax({
    url: "/monitoring/update_hasil_pemeriksaan",
    method: "POST",
    contentType: "application/json",
    data: JSON.stringify(requestData),
    success: function (response) {
      console.log("✅ Save successful:", response);
      alert(`✅ ${response.message || `Berhasil menyimpan ${updates.length} perubahan!`}`);
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

// $("#saveChangesBtn").on("click", function () {
//   const btn = $(this);
//   let updates = [];
//   let tableName = $("#detailModal").attr("data-table");
  
//   let blth = $("#detailModal").attr("data-blth");
//   let unitup = $("#detailModal").attr("data-unitup");

//   // ✅ FALLBACK 1: Ambil dari URL parameter secara langsung
//   if (!unitup || unitup.trim() === "") {
//     const urlParams = new URLSearchParams(window.location.search);
//     unitup = urlParams.get('unitup') || '';
//     console.log("⚠️ Using UNITUP from URL directly:", unitup);
//   }

//   // ✅ FALLBACK 2: Ambil dari row pertama di table
//   if (!unitup || unitup.trim() === "") {
//     const firstRow = $("#detailTableBody tr:first");
//     if (firstRow.length) {
//       // Asumsi UNITUP ada di kolom ke-2 (index 1, karena kolom 0 adalah No)
//       unitup = firstRow.find("td").eq(1).text().trim();
//       console.log("⚠️ Using UNITUP from table row:", unitup);
//     }
//   }

//   console.log("🔄 Preparing to save changes...");
//   console.log("📋 Table:", tableName);
//   console.log("📅 BLTH:", blth);
//   console.log("🏢 UNITUP:", unitup);

//   // ✅ VALIDASI dengan pesan yang lebih informatif
//   if (!blth || blth.trim() === "") {
//     alert("⚠️ BLTH tidak ditemukan!\n\nSilakan:\n1. Tutup modal ini\n2. Buka ulang dari tabel utama\n3. Pastikan URL memiliki parameter ?blth=YYYYMM");
//     return;
//   }

//   if (!unitup || unitup.trim() === "") {
//     alert("⚠️ UNITUP tidak ditemukan!\n\nSilakan:\n1. Pastikan URL memiliki parameter ?unitup=XXXXX\n2. Atau tutup dan buka ulang modal\n3. Jika masalah berlanjut, hubungi administrator");
//     return;
//   }

//   // Lanjutkan dengan building updates array
//   $("#detailTableBody tr").each(function () {
//     let row = $(this);
    
//     let idpel = row.find("input.stan-verifikasi-input").data("idpel");
//     if (!idpel) {
//       let hasilSelect = row.find("select[name^='hasil_']");
//       if (hasilSelect.length) {
//         idpel = hasilSelect.attr("name").split("_")[1];
//       }
//     }
    
//     if (!idpel) {
//       console.warn("⚠️ No IDPEL found for row, skipping");
//       return;
//     }

//     let hasil = row.find(`select[name="hasil_${idpel}"]`).val() || "";
//     let tindak = row.find(`textarea[name="tindak_${idpel}"]`).val() || "";
//     let stan_verifikasi = row.find(`input.stan-verifikasi-input[data-idpel="${idpel}"]`).val() || "";

//     updates.push({
//       IDPEL: idpel,
//       BLTH: blth,
//       UNITUP: unitup,
//       HASIL: hasil,
//       TINDAK: tindak,
//       STAN: stan_verifikasi
//     });
//   });

//   console.log(`📊 Total updates: ${updates.length}`);
//   console.log("📦 Updates data:", updates);

//   if (updates.length === 0) {
//     alert("⚠️ Tidak ada data untuk disimpan");
//     return;
//   }

//   btn
//     .prop("disabled", true)
//     .html(
//       '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Menyimpan...'
//     );

//   const requestData = {
//     table: tableName,
//     updates: updates
//   };

//   console.log("🚀 Sending request:", requestData);

//   $.ajax({
//     url: "/monitoring/update_hasil_pemeriksaan",
//     method: "POST",
//     contentType: "application/json",
//     data: JSON.stringify(requestData),
//     success: function (response) {
//       console.log("✅ Save successful:", response);
//       alert(`✅ ${response.message || 'Data berhasil disimpan!'}`);
//       location.reload();
//     },
//     error: function (xhr, status, error) {
//       console.error("❌ Save failed:", {
//         status: xhr.status,
//         statusText: xhr.statusText,
//         response: xhr.responseText,
//         error: error
//       });
      
//       let errorMsg = "Gagal menyimpan data.";
//       if (xhr.responseJSON && xhr.responseJSON.message) {
//         errorMsg = xhr.responseJSON.message;
//       } else if (xhr.responseText) {
//         try {
//           const errData = JSON.parse(xhr.responseText);
//           errorMsg = errData.message || errorMsg;
//         } catch (e) {
//           errorMsg = xhr.responseText.substring(0, 100);
//         }
//       }
      
//       alert(`❌ ${errorMsg}`);
//       btn.prop("disabled", false).html("Simpan Perubahan");
//     },
//   });
// });
});