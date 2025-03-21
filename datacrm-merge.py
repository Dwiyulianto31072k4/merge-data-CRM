import streamlit as st
import pandas as pd
import msoffcrypto
import os
import base64
from io import BytesIO
import tempfile

# Konfigurasi halaman
st.set_page_config(
    page_title="Pipeline Data Merge",
    page_icon="🔄",
    layout="wide"
)

st.title("Pipeline Data Merge - Telesales FIF")
st.write("Upload file Excel yang diproteksi password untuk menggabungkan dan memfilter data berdasarkan customer_no terbaru.")

# Sidebar untuk konfigurasi
with st.sidebar:
    st.header("Konfigurasi")
    password = st.text_input("Password File Excel", type="password", value="202502")
    st.info("Pastikan password sesuai dengan file yang diupload")
    
    st.subheader("Petunjuk Penggunaan")
    st.markdown("""
    1. Upload satu atau beberapa file Excel yang diproteksi password
    2. Tentukan sheet untuk masing-masing file (LOAD atau default)
    3. Klik tombol "Proses Data" untuk menggabungkan file
    4. Download hasil penggabungan yang sudah difilter
    """)

# Fungsi untuk download dataframe sebagai file Excel
def get_excel_download_link(df, filename):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    excel_data = output.getvalue()
    b64 = base64.b64encode(excel_data).decode('utf-8')
    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}">Download {filename}</a>'
    return href

# Fungsi untuk memproses file dengan password
def process_encrypted_excel(uploaded_file, password, sheet_name):
    try:
        # Simpan file upload ke temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            tmp.write(uploaded_file.getvalue())
            temp_path = tmp.name
        
        # Dekripsi file
        decrypted_file = BytesIO()
        with open(temp_path, "rb") as f:
            office_file = msoffcrypto.OfficeFile(f)
            office_file.load_key(password=password)
            office_file.decrypt(decrypted_file)
        
        # Hapus file temporary
        os.unlink(temp_path)
        
        # Baca file yang sudah didekripsi
        decrypted_file.seek(0)
        df = pd.read_excel(decrypted_file, sheet_name=sheet_name, engine="openpyxl")
        
        # Standardisasi nama kolom
        df.columns = df.columns.str.strip().str.lower().str.replace("periode call", "period_call").str.replace(" ", "_")
        
        # Konversi period_call ke format tanggal
        if "period_call" in df.columns:
            df["period_call"] = pd.to_datetime(df["period_call"], errors="coerce", dayfirst=True).dt.date
        
        # Tambahkan informasi sumber file
        df["source_file"] = uploaded_file.name
        
        return df, None
    except Exception as e:
        return None, str(e)

# Container untuk upload file
upload_container = st.container()
with upload_container:
    st.subheader("Upload File Excel")
    uploaded_files = st.file_uploader("Pilih satu atau beberapa file Excel", type=["xlsx"], accept_multiple_files=True)

    # Jika ada file yang diupload
    if uploaded_files:
        st.success(f"{len(uploaded_files)} file telah diupload")
        
        # Opsi untuk sheet yang akan dibaca
        sheet_options = {}
        for file in uploaded_files:
            sheet_choice = st.radio(
                f"Pilih sheet untuk {file.name}:",
                ["Default (0)", "LOAD"],
                key=f"sheet_choice_{file.name}"
            )
            sheet_options[file.name] = "LOAD" if sheet_choice == "LOAD" else 0

# Proses data ketika tombol diklik
if uploaded_files:
    if st.button("Proses Data", key="process_button", type="primary"):
        with st.spinner("Sedang memproses data..."):
            # Inisialisasi untuk menyimpan dataframe dan log
            data_frames = []
            log_messages = []
            
            # Progress bar
            progress_bar = st.progress(0)
            
            # Proses setiap file yang diupload
            for i, uploaded_file in enumerate(uploaded_files):
                sheet_to_read = sheet_options[uploaded_file.name]
                st.text(f"Memproses: {uploaded_file.name} (Sheet: {sheet_to_read})")
                
                # Proses file
                df, error = process_encrypted_excel(uploaded_file, password, sheet_to_read)
                
                if df is not None:
                    data_frames.append(df)
                    log_messages.append(f"✅ Berhasil membaca: {uploaded_file.name} (Sheet: {sheet_to_read})")
                    log_messages.append(f"   Total baris: {df.shape[0]}")
                else:
                    log_messages.append(f"❌ Gagal membaca {uploaded_file.name}: {error}")
                
                # Update progress bar
                progress_bar.progress((i + 1) / len(uploaded_files))
            
            # Tampilkan log
            st.subheader("Log Proses")
            for msg in log_messages:
                st.text(msg)
            
            # Jika ada data yang berhasil dibaca
            if data_frames:
                # Gabungkan semua dataframe
                st.subheader("Hasil Penggabungan")
                merged_df = pd.concat(data_frames, ignore_index=True)
                
                # Cek kolom penting
                if "period_call" in merged_df.columns and "customer_no" in merged_df.columns:
                    # Pastikan format tanggal konsisten
                    merged_df["period_call"] = pd.to_datetime(merged_df["period_call"], errors="coerce").dt.date
                    
                    # Tampilkan tanggal unik sebelum sorting
                    unique_dates_before = merged_df["period_call"].unique()
                    st.write("Tanggal unik sebelum sorting:", unique_dates_before)
                    
                    # Urutkan berdasarkan tanggal
                    merged_df = merged_df.sort_values(by="period_call", ascending=False)
                    
                    # Tampilkan tanggal unik setelah sorting
                    unique_dates_after = merged_df["period_call"].unique()
                    st.write("Tanggal unik setelah sorting:", unique_dates_after)
                    
                    # Filter untuk menyimpan data terbaru untuk setiap customer
                    total_before_filter = merged_df.shape[0]
                    unique_customers = merged_df.drop_duplicates(subset="customer_no", keep="first")
                    total_after_filter = unique_customers.shape[0]
                    
                    # Tampilkan statistik
                    st.write(f"Jumlah data sebelum filtering: {total_before_filter}")
                    st.write(f"Jumlah data setelah filtering berdasarkan customer_no terbaru: {total_after_filter}")
                    st.write(f"Jumlah duplicate customer yang difilter: {total_before_filter - total_after_filter}")
                    
                    # Tampilkan preview data
                    st.subheader("Preview Data Hasil Filtering")
                    st.dataframe(unique_customers.head(10))
                    
                    # Tampilkan link download
                    st.subheader("Download Hasil")
                    st.markdown(get_excel_download_link(unique_customers, "filtered_unique_customers.xlsx"), unsafe_allow_html=True)
                    
                    # Tampilkan opsi download untuk data sebelum filtering juga
                    st.markdown(get_excel_download_link(merged_df, "all_merged_data.xlsx"), unsafe_allow_html=True)
                else:
                    st.error("Kolom 'period_call' atau 'customer_no' tidak ditemukan setelah merge. Pastikan nama kolom seragam.")
            else:
                st.error("Tidak ada file yang berhasil dibaca.")
