 -- Tạo bảng fact_kpi_asm  để lưu trữ bảng kpi_asm_data (chuyển từ cột sang dòng bằng cách sử dụng unnest)
	create table fact_kpi_asm (
	  month_key int8  ,
	  area_name varchar(1024) ,
	  sale_name varchar(1024),
	  email varchar(1024),
	  month_col int4  ,
	  loan_to_new int8  ,
	  psdn int4  ,
	  app_approved int4  ,
	  app_in int4  ,
	  approval_rate float4 
	)
	
insert into fact_kpi_asm 
	select 
	  month_key,
	  area_name,
	  sale_name,
	  email,
	  month_col,
	  loan_to_new,
	  psdn,
	  app_approved,
	  app_in,
	  approval_rate
	FROM kpi_asm_data kad,LATERAL (
	  SELECT *
	  FROM UNNEST(
	    ARRAY[1,2,3,4,5],
	    ARRAY[jan_loan, feb_loan, mar_loan, apr_loan, may_loan],
	    ARRAY[jan_psdn, feb_psdn, mar_psdn, apr_psdn, may_psdn],
	    ARRAY[jan_app_approved, feb_app_approved, mar_app_approved, apr_app_approved, may_app_approved],
	    ARRAY[jan_app_in, feb_app_in, mar_app_in, apr_app_in, may_app_in],
	    ARRAY[jan_approval_rate, feb_approval_rate, mar_approval_rate, apr_approval_rate, may_approval_rate]
	  ) AS t(
	    month_col,
	    loan_to_new,
	    psdn,
	    app_approved,
	    app_in,
	    approval_rate
	  )
	) AS unnested_data
	ORDER BY sale_name , month_col asc


-- tạo bảng mới fact_txn_month_data để lưu trữ thông tin của bảng fact_txn_month_raw_data
-- Phân tách cột analysis_code thành các cột network_unit , region_code , area_code , province_code , pos_code 
	CREATE TABLE fact_txn_month_data (
		transaction_date date NULL,
		account_code varchar(1024) NULL,
		account_description varchar(1024) NULL,
		analysis_code varchar(1024) NULL,
		network_unit varchar(1024) NULL,
		region_code varchar(1024) NULL, -- lưu vùng miền (Bắc - Trung - Nam )
		area_code varchar(1024) NULL, -- lưu khu vực (Đông Nam Bộ , Bắc Nam Bộ ...)
		province_code varchar(1024) NULL, -- lưu tỉnh thành (Bình Phước , Đồng Nai , ...)
		pos_code varchar(1024 ) NULL, -- lưu chi tiết Pos_code của khi vực
		amount int8 NULL,
		d_c varchar(1024) NULL
	);
insert into fact_txn_month_data 
	select transaction_date , account_code  , account_description  , analysis_code ,
			-- Tách theo dấu chấm
		    split_part(analysis_code, '.', 1) AS network_unit,
		    split_part(analysis_code, '.', 2) AS region_code,
		    split_part(analysis_code, '.', 3) AS area_code,
		    split_part(analysis_code, '.', 4) AS province_code,
		    -- Tách POS code bằng REGEXP
		    substring(analysis_code FROM 'POS_[0-9]+') AS pos_code,
			amount , d_c 
	from fact_txn_month_raw_data ftmrd 



 -- Tạo các bảng dimension để lưu trữ các thông tin (có thể tái sử dụng )
-- Tạo bảng lưu mã code của từng vùng miền (Bắc Trung Nam )
	CREATE TABLE dim_region_table (
	    region_id TEXT PRIMARY KEY,  -- Ví dụ: '01', '02', '03'
	    region_name TEXT NOT NULL    -- Ví dụ: 'Miền Bắc'
	);
	INSERT INTO dim_region_table  (region_id, region_name) 
	values
	('00', 'Trụ sở chính'),
	('01', 'Miền Bắc'),
	('02', 'Miền Trung'),
	('03', 'Miền Nam');

-- Tạo bảng lưu mã code của từng khu vực 
CREATE TABLE dim_area_table  (
    area_code CHAR(1) PRIMARY KEY,      -- A–H
    area_name TEXT NOT NULL,            -- Ví dụ: 'Đông Bắc Bộ'
    region_id TEXT NOT NULL,            -- FK to region_table.region_id
    FOREIGN KEY (region_id) REFERENCES dim_region_table(region_id)
);

INSERT INTO dim_area_table (area_code, area_name, region_id) VALUES
	('A', 'Hội Sở', '00'),              -- thuộc Trụ sở chính 
	('B', 'Đông Bắc Bộ', '01'),			-- thuộc Miền Bắc
	('C', 'Tây Bắc Bộ', '01'),
	('D', 'Đồng Bằng Sông Hồng', '01'),
	('E', 'Bắc Trung Bộ', '02'),		-- thuộc Miền Trung
	('F', 'Nam Trung Bộ', '02'),        
	('G', 'Tây Nam Bộ', '03'),          -- thuộc Miền Nam
	('H', 'Đông Nam Bộ', '03');
-- Tạo bảng lưu trữ mã code của từng tỉnh thành 
CREATE TABLE dim_province_table (
    province_id TEXT PRIMARY KEY,       -- Ví dụ: '01', '79' (mã tỉnh)
    province_name TEXT NOT NULL,        -- 'Hà Nội', 'TP.HCM', ...
    area_code CHAR(1) NOT NULL,         -- FK to area.area_code
    FOREIGN KEY (area_code) REFERENCES dim_area_table (area_code)
);
INSERT INTO dim_province_table (province_id, province_name, area_code) VALUES
	('00', 'Hội Sở', 'A'),
	('01', 'An Giang', 'G'),
	('02', 'Bắc Giang', 'B'),
	('03', 'Bắc Kạn', 'B'),
	('04', 'Bạc Liêu', 'G'),
	('05', 'Bắc Ninh', 'D'),
	('06', 'Bến Tre', 'G'),
	('07', 'Bình Định', 'F'),
	('08', 'Bình Dương', 'H'),
	('09', 'Bình Phước', 'H'),
	('10', 'Bình Thuận', 'F'),
	('11', 'Cà Mau', 'G'),
	('12', 'Cần Thơ', 'G'),
	('13', 'Cao Bằng', 'B'),
	('14', 'Đà Nẵng', 'F'),
	('15', 'Đắk Lắk', 'F'),
	('16', 'Đắk Nông', 'F'),
	('17', 'Điện Biên', 'C'),
	('18', 'Đồng Nai', 'H'),
	('19', 'Đồng Tháp', 'G'),
	('20', 'Gia Lai', 'F'),
	('21', 'Hà Giang', 'B'),
	('22', 'Hà Nam', 'D'),
	('23', 'Hà Nội', 'D'),
	('24', 'Hà Tĩnh', 'E'),
	('25', 'Hải Dương', 'D'),
	('26', 'Hải Phòng', 'D'),
	('27', 'Hậu Giang', 'G'),
	('28', 'Hồ Chí Minh', 'H'),
	('29', 'Hòa Bình', 'C'),
	('30', 'Hưng Yên', 'D'),
	('31', 'Khánh Hòa', 'F'),
	('32', 'Kiên Giang', 'G'),
	('33', 'Kon Tum', 'F'),
	('34', 'Lai Châu', 'C'),
	('35', 'Lâm Đồng', 'F'),
	('36', 'Lạng Sơn', 'B'),
	('37', 'Lào Cai', 'C'),
	('38', 'Long An', 'G'),
	('39', 'Nam Định', 'D'),
	('40', 'Nghệ An', 'E'),
	('41', 'Ninh Bình', 'D'),
	('42', 'Ninh Thuận', 'F'),
	('43', 'Phú Thọ', 'B'),
	('44', 'Phú Yên', 'F'),
	('45', 'Quảng Bình', 'E'),
	('46', 'Quảng Nam', 'F'),
	('47', 'Quảng Ngãi', 'F'),
	('48', 'Quảng Ninh', 'B'),
	('49', 'Quảng Trị', 'E'),
	('50', 'Sóc Trăng', 'G'),
	('51', 'Sơn La', 'C'),
	('52', 'Tây Ninh', 'H'),
	('53', 'Thái Bình', 'D'),
	('54', 'Thái Nguyên', 'B'),
	('55', 'Thanh Hóa', 'E'),
	('56', 'Huế', 'E'),
	('57', 'Tiền Giang', 'G'),
	('58', 'Trà Vinh', 'G'),
	('59', 'Tuyên Quang', 'B'),
	('60', 'Vĩnh Long', 'G'),
	('61', 'Vĩnh Phúc', 'D'),
	('62', 'Bà Rịa - Vũng Tàu', 'H'),
	('63', 'Yên Bái', 'C');


-- Tạo bảng lưu trữ 
CREATE TABLE rate_allocation_area_month  (
  rate_month   int4        NOT NULL ,  -- lưu trữ theo từng tháng 
  method_code  TEXT        NOT NULL,                      -- tự nhập
  method_name  TEXT        NOT null,					  -- lưu trữ tên của trường hợp 
  area_code    CHAR(1)     NOT NULL REFERENCES  dim_area_table(area_code),  -- code của khu vực 
  ratio        NUMERIC(18,10) NOT NULL,		-- tỉ lệ theo từng khu vực 
  FOREIGN KEY (area_code) REFERENCES dim_area_table (area_code)
);



------------------Tạo bảng dimension lưu trữ thông tin về từng loại thuế 
CREATE TABLE dim_pnl_structure (
    pnl_id         SERIAL PRIMARY KEY,            -- ID tự tăng
    pnl_code       TEXT NOT NULL,                 -- Mã chỉ tiêu
    pnl_name       TEXT NOT NULL,                 -- Tên chỉ tiêu
    pnl_parent_id  INT,                           -- ID của chỉ tiêu cha (-1 nếu không có)
    pnl_level      INT NOT NULL,                  -- Cấp bậc: 0 = root, 1 = nhóm, 2 = chi tiết
    sortorder      INT NOT NULL                   -- Để sắp xếp 
);

INSERT INTO dim_pnl_structure (pnl_code, pnl_name, pnl_parent_id, pnl_level, sortorder) VALUES
-- Level 0
('P1', 'A. Lợi nhuận trước thuế', -1, 0, 1000000),

-- Level 1: Thu nhập từ hoạt động thẻ
('P1_01', '1.Thu nhập từ hoạt động thẻ', 20, 1, 1010000),
('P1_01_01', 'Lãi trong hạn', 2, 2, 1010100),
('P1_01_02', 'Lãi quá hạn', 2, 2, 1010200),
('P1_01_03', 'Phí Bảo hiểm', 2, 2, 1010300),
('P1_01_04', 'Phí tăng hạn mức', 2, 2, 1010400),
('P1_01_05', 'Phí thanh toán chậm, thu từ ngoại bảng, khác', 2, 2, 1010500),

-- Level 1: Chi phí thuần KDV
('P1_02', '2.Chi phí thuần KDV', 20, 1, 1020000),
('P1_02_01', 'DT Nguồn vốn', 8, 2, 1020100),
('P1_02_02', 'CP vốn TT 2', 8, 2, 1020200),
('P1_02_03', 'CP vốn TT 1', 8, 2, 1020300),
('P1_02_04', 'CP vốn CCTG', 8, 2, 1020400),

-- Level 1: Chi phí thuần hoạt động khác
('P1_03', '3.Chi phí thuần hoạt động khác', 20, 1, 1030000),
('P1_03_01', 'DT Fintech', 13, 2, 1030100),
('P1_03_02', 'DT tiểu thương, cá nhân', 13, 2, 1030200),
('P1_03_03', 'DT Kinh doanh', 13, 2, 1030300),
('P1_03_04', 'CP hoa hồng', 13, 2, 1030400),
('P1_03_05', 'CP thuần KD khác', 13, 2, 1030500),
('P1_03_06', 'CP hợp tác kd tàu (net)', 13, 2, 1030600),

-- Level 1: Tổng thu nhập hoạt động
('P1_04', '4.Tổng thu nhập hoạt động', 1, 1, 1040000),

-- Level 1: Tổng chi phí hoạt động
('P1_05', '5.Tổng chi phí hoạt động', 1, 1, 1050000),
('P1_05_01', 'CP thuế, phí', 21, 2, 1050100),
('P1_05_02', 'CP nhân viên', 21, 2, 1050200),
('P1_05_03', 'CP quản lý', 21, 2, 1050300),
('P1_05_04', 'CP tài sản', 21, 2, 1050400),
-- Level 1: Chi phí dự phòng
('P1_06', '6.Chi phí dự phòng', 1, 1, 1060000),

-- Level 0: Số lượng nhân sự (Sale Manager)
('P2', 'B. Số lượng nhân sự (Sale Manager)', -1, 0, 2000000),

-- Level 0: Chỉ số tài chính
('P3', 'C. Chỉ số tài chính', -1, 0, 3000000),
('P3_01', 'CIR (%)', 28, 1, 3001000),
('P3_02', 'Margin (%)', 28, 1, 3002000),
('P3_03', 'Hiệu suất trên/vốn (%)', 28, 1, 3003000),
('P3_04', 'Hiệu suất BQ/ Nhân sự', 28, 1, 3004000);


-- Bảng báo cáo về finance 
create table report_finance_table(
	month_key int4 not null ,
	id int not null ,
	area_code char(2),
	amount float8 
)



-- Tạo bảng lưu trữ báo cáo ASM
CREATE TABLE report_asm_final_table (
    month_key           INT4 NOT NULL,
    area_name           TEXT,
    email               TEXT,
    tong_diem           NUMERIC,
    rank_final          INT,

    -- Chỉ tiêu Quy mô
    ltn_avg             NUMERIC,
    rank_ltn_avg        INT,
    psdn_avg            NUMERIC,
    rank_psdn_avg       INT,
    approval_rate_avg   NUMERIC,
    rank_approval_rate_avg INT,
    npl_truoc_wo_luy_ke NUMERIC,
    rank_npl_truoc_wo_luy_ke INT,
    diem_quymo          NUMERIC,
    rank_ptkd           INT,

    -- Chỉ tiêu Tài chính
    cir                 NUMERIC,
    rank_cir            INT,
    margin              NUMERIC,
    rank_margin         INT,
    hs_von              NUMERIC,
    rank_hs_von         INT,
    hsbp_nhan_su        NUMERIC,
    rank_hsbq_nhan_su   INT,
    diem_fin            NUMERIC,
    rank_fin            INT
);



-- Tạo bảng log ghi lại lỗi khi chạy
CREATE TABLE error_log (
    log_id       SERIAL PRIMARY KEY,
    proc_name    TEXT NOT NULL,                     -- tên procedure con
    run_month    INT4 NOT NULL,                     -- tháng chạy
    error_message TEXT,                             -- chỉ ghi khi FAIL
    start_time   TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    end_time     TIMESTAMPTZ
);

