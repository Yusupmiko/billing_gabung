-- phpMyAdmin SQL Dump
-- version 5.2.0
-- https://www.phpmyadmin.net/
--
-- Host: localhost:3306
-- Generation Time: Nov 12, 2025 at 07:59 AM
-- Server version: 8.0.30
-- PHP Version: 8.1.10

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `billing_gabungan`
--

-- --------------------------------------------------------

--
-- Table structure for table `audit_log`
--

CREATE TABLE `audit_log` (
  `id` bigint NOT NULL,
  `table_name` varchar(50) DEFAULT NULL,
  `record_id` bigint DEFAULT NULL,
  `idpel` varchar(12) DEFAULT NULL,
  `blth` varchar(6) DEFAULT NULL,
  `column_changed` varchar(50) DEFAULT NULL,
  `old_value` text,
  `new_value` text,
  `changed_by` varchar(50) DEFAULT NULL,
  `changed_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `billing`
--

CREATE TABLE `billing` (
  `BLTH` varchar(6) NOT NULL,
  `UNITUP` varchar(50) NOT NULL,
  `IDPEL` varchar(20) NOT NULL,
  `NAMA` varchar(100) DEFAULT NULL,
  `TARIF` varchar(10) DEFAULT NULL,
  `DAYA` bigint DEFAULT NULL,
  `SLALWBP` bigint DEFAULT NULL,
  `LWBPCABUT` bigint DEFAULT NULL,
  `SELISIH_STAN_BONGKAR` bigint DEFAULT NULL,
  `LWBPPASANG` bigint DEFAULT NULL,
  `KWH_SEKARANG` int DEFAULT '0',
  `KWH_1_BULAN_LALU` int DEFAULT '0',
  `KWH_2_BULAN_LALU` int DEFAULT '0',
  `SAHLWBP` bigint DEFAULT NULL,
  `DELTA_PEMKWH` bigint DEFAULT NULL,
  `PERSEN` varchar(10) DEFAULT NULL,
  `JAM_NYALA` decimal(10,2) DEFAULT NULL,
  `JAMNYALA600` varchar(50) DEFAULT NULL,
  `NOMORKWH` varchar(50) DEFAULT NULL,
  `GRAFIK` text,
  `FOTO_AKHIR` text,
  `FOTO_LALU` text,
  `FOTO_LALU2` text,
  `FOTO_3BLN` text,
  `HASIL_PEMERIKSAAN` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `STAN_VERIFIKASI` text,
  `TINDAK_LANJUT` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `KET` varchar(50) DEFAULT NULL,
  `KDKELOMPOK` varchar(20) DEFAULT NULL,
  `DLPD` text,
  `DLPD_3BLN` text,
  `DLPD_HITUNG` text,
  `MARKING_KOREKSI` tinyint DEFAULT NULL,
  `updated_by` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `dil`
--

CREATE TABLE `dil` (
  `id` bigint NOT NULL,
  `idpel` varchar(12) NOT NULL,
  `NOMORKWH` varchar(50) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `dpm`
--

CREATE TABLE `dpm` (
  `BLTH` varchar(6) NOT NULL,
  `UNITUP` varchar(20) DEFAULT NULL,
  `IDPEL` varchar(20) NOT NULL,
  `NAMA` varchar(100) DEFAULT NULL,
  `TARIF` varchar(10) DEFAULT NULL,
  `DAYA` int DEFAULT NULL,
  `SLALWBP` bigint DEFAULT NULL,
  `LWBPCABUT` bigint DEFAULT NULL,
  `LWBPPASANG` bigint DEFAULT NULL,
  `SAHLWBP` bigint DEFAULT NULL,
  `LWBPPAKAI` bigint DEFAULT NULL,
  `DLPD` varchar(50) DEFAULT NULL,
  `KDKELOMPOK` varchar(10) DEFAULT NULL,
  `CREATED_AT` datetime DEFAULT NULL,
  `updated_by` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `tb_user`
--

CREATE TABLE `tb_user` (
  `id_user` int NOT NULL,
  `unitup` varchar(20) NOT NULL,
  `nama_ulp` varchar(100) DEFAULT NULL,
  `username` varchar(50) NOT NULL,
  `password` varchar(255) NOT NULL,
  `role` enum('ULP','UP3') DEFAULT 'ULP',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Dumping data for table `tb_user`
--

INSERT INTO `tb_user` (`id_user`, `unitup`, `nama_ulp`, `username`, `password`, `role`, `created_at`) VALUES
(1, 'UP3', 'Administrator UP3', 'admin', '240be518fabd2724ddb6f04eeb1da5967448d7e831c08c8fa822809f74c720a9', 'UP3', '2025-10-31 03:44:06'),
(4, '21210', 'ULP PEMANGKAT', 'pmkt123', 'ecc3ef02afa2baf68e42ff24d3ada7b685e8e2f32225e42fdf50ae1fc11f783f', 'ULP', '2025-11-07 05:31:32'),
(5, '21220', 'ULP SAMBAS', 'sbs123', 'ccd6773f1c5258ffa6d1789ce2a899423051c3f1d7650bfb7db63438455f1728', 'ULP', '2025-11-07 05:32:25'),
(6, '21250', 'ULP SUNGAI DURI', 'sdr123', '1b1517862d3460f0940ed6e7c315571e84f4aad567f8afe26f7f80f20ba3e677', 'ULP', '2025-11-07 05:33:04'),
(7, '21200', 'ULP SINGKAWANG KOTA', 'skw123', '838888b79e2e01957c4a2f4ce785ec2f2bebfe9b4d217bd8ff78774532eece46', 'ULP', '2025-11-07 05:34:08'),
(8, '21230', 'ULP BENGKAYANG', 'bkg123', 'df18a9c46fbf35e95718a7b02821f55766a3c4dbcf55ec1754e73408e37ccbbe', 'ULP', '2025-11-07 05:35:27'),
(9, '21240', 'ULP SEKURA', 'skr123', 'ef90ff6dba17984a19d756c986586eb3dd06079071d5b755dacbac9723916384', 'ULP', '2025-11-07 05:36:50');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `audit_log`
--
ALTER TABLE `audit_log`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_idpel_blth` (`idpel`,`blth`),
  ADD KEY `idx_changed_at` (`changed_at`);

--
-- Indexes for table `billing`
--
ALTER TABLE `billing`
  ADD PRIMARY KEY (`BLTH`,`UNITUP`,`IDPEL`),
  ADD KEY `idx_billing_unitup` (`UNITUP`),
  ADD KEY `idx_billing_blth` (`BLTH`),
  ADD KEY `idx_billing_ket` (`KET`),
  ADD KEY `idx_updated_by` (`updated_by`),
  ADD KEY `idx_blth_unitup` (`BLTH`,`UNITUP`);

--
-- Indexes for table `dil`
--
ALTER TABLE `dil`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_idpel` (`idpel`),
  ADD KEY `idx_nomet` (`NOMORKWH`);

--
-- Indexes for table `dpm`
--
ALTER TABLE `dpm`
  ADD PRIMARY KEY (`BLTH`,`IDPEL`,`updated_by`),
  ADD KEY `idx_unitup_blth` (`UNITUP`,`BLTH`),
  ADD KEY `idx_dpm_updated_by` (`updated_by`);

--
-- Indexes for table `tb_user`
--
ALTER TABLE `tb_user`
  ADD PRIMARY KEY (`id_user`),
  ADD UNIQUE KEY `username` (`username`),
  ADD UNIQUE KEY `unique_unitup` (`unitup`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `audit_log`
--
ALTER TABLE `audit_log`
  MODIFY `id` bigint NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `dil`
--
ALTER TABLE `dil`
  MODIFY `id` bigint NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `tb_user`
--
ALTER TABLE `tb_user`
  MODIFY `id_user` int NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=11;

DELIMITER $$
--
-- Events
--
CREATE DEFINER=`root`@`localhost` EVENT `evt_cleanup_old_dpm` ON SCHEDULE EVERY 1 DAY STARTS '2025-10-31 10:26:32' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
    -- Hapus data DPM lebih dari 6 bulan
    DELETE FROM dpm 
    WHERE CAST(blth AS UNSIGNED) < CAST(DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 6 MONTH), '%Y%m') AS UNSIGNED);
END$$

DELIMITER ;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
