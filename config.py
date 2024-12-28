import mysql.connector
from mysql.connector import Error

def get_db_connection():
    """Kết nối tới cơ sở dữ liệu MySQL."""
    try:
        conn = mysql.connector.connect(
            host="localhost",      
            user="root",     
            password="123456789", 
            database="products"  
        )
        if conn.is_connected():
            print("Kết nối đến cơ sở dữ liệu thành công.")
            return conn, conn.cursor()
    except Error as e:
        print(f"Lỗi khi kết nối đến MySQL: {e}")
        return None, None

def create_table():
    command = """
    CREATE TABLE IF NOT EXISTS products (
        id INT PRIMARY KEY,
        name TEXT,
        url_key TEXT,
        price DECIMAL(15,2),
        description LONGTEXT,
        images_url JSON
    )
    """
    connection, cursor = get_db_connection()

    if connection and cursor:
        cursor.execute(command)
        connection.commit()
        print("Bảng 'products' đã được tạo thành công ")
        cursor.close()
        connection.close()

if __name__ == "__main__":
    create_table()