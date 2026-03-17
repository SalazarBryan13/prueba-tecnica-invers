import pandas as pd
from sqlalchemy import create_engine

class DatabaseManager:
    """Clase para centralizar todas las operaciones de base de datos (Patrón DAO)."""
    
    def __init__(self, db_uri: str):
        self.engine = create_engine(db_uri)

    def check_data_exists(self) -> bool:
        """Verifica si hay registros en la tabla de hechos."""
        try:
            with self.engine.connect() as conn:
                query = "SELECT COUNT(*) FROM fact_admission"
                count = pd.read_sql(query, conn).iloc[0, 0]
                return count > 0
        except Exception as e:
            print(f"Error checking data existence: {e}")
            return False

    def get_executive_kpis(self) -> pd.Series:
        """Obtiene los KPIs globales para el reporte proactivo."""
        query = """
        SELECT 
            COUNT(*) as total_records,
            SUM(f.billing_amount) as total_billing,
            (SELECT COUNT(*) FROM fact_admission WHERE test_results = 'Abnormal') * 100.0 / COUNT(*) as abnormal_rate,
            AVG(d2.full_date - d1.full_date) as avg_stay
        FROM fact_admission f
        JOIN dim_date d1 ON f.admission_date_id = d1.date_id
        JOIN dim_date d2 ON f.discharge_date_id = d2.date_id
        """
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn).iloc[0]

    def get_top_hospitals_revenue(self, limit: int = 5) -> pd.DataFrame:
        """Obtiene los hospitales con mayor facturación."""
        query = f"""
        SELECT h.hospital_name, SUM(f.billing_amount) as total 
        FROM fact_admission f 
        JOIN dim_hospital h ON f.hospital_id = h.hospital_id 
        GROUP BY h.hospital_name ORDER BY total DESC LIMIT {limit}
        """
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)

    def get_report_data(self, report_type: str) -> pd.DataFrame:
        """Obtiene datos específicos para los diferentes tipos de reportes Qx."""
        queries = {
            'q1_seasonality': """
                SELECT d.year, d.month, COUNT(*) as admisiones
                FROM fact_admission f
                JOIN dim_date d ON f.admission_date_id = d.date_id
                GROUP BY d.year, d.month ORDER BY d.year, d.month
            """,
            'q2_meds': """
                SELECT m.medication_name, SUM(f.billing_amount) as total
                FROM fact_admission f
                JOIN dim_medication m ON f.medication_id = m.medication_id
                GROUP BY m.medication_name ORDER BY total DESC LIMIT 10
            """,
            'q5_insurance': """
                SELECT i.provider_name, SUM(f.billing_amount) as total
                FROM fact_admission f
                JOIN dim_insurance i ON f.insurance_id = i.insurance_id
                GROUP BY i.provider_name ORDER BY total DESC
            """,
            'q_doctors': """
                SELECT d.doctor_name, COUNT(*) as casos, SUM(f.billing_amount) as total
                FROM fact_admission f
                JOIN dim_doctor d ON f.doctor_id = d.doctor_id
                GROUP BY d.doctor_name ORDER BY total DESC LIMIT 10
            """
        }
        
        if report_type not in queries:
            raise ValueError(f"Report type '{report_type}' not supported.")
            
        with self.engine.connect() as conn:
            return pd.read_sql(queries[report_type], conn)
