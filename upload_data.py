import json
import os
import mysql.connector
from mysql.connector import Error
from config import get_db_connection  

def insert_product(cursor, product):
    #chèn một sản phầm vào 
    command = """
    INSERT INTO products (id, name, url_key, price, description, images_url)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(command, (
        product['id'],
        product['name'],
        product['url_key'],
        product['price'],
        product['description'],
        json.dumps(product['images_url'])  # Chuyển danh sách URL thành chuỗi JSON
    ))

def load_json_files_to_db(directory):
    # Kết nối cơ sở dữ liệu
    connection, cursor = get_db_connection()
    if connection is None or cursor is None:
        print("Không thể kết nối tới cơ sở dữ liệu.")
        return
    total_products = 0
    try:
        for filename in os.listdir(directory):
            # Kiểm tra các tệp JSON có tên bắt đầu bằng 'products_'
            if filename.startswith('products_') and filename.endswith('.json'):
                file_path = os.path.join(directory, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    products = json.load(f)
                    for product in products:
                        try:
                            insert_product(cursor, product)  # Chèn sản phẩm
                            total_products += 1
                            connection.commit()  # Commit mỗi khi chèn thành công
                            print(f"Đã chèn sản phẩm ID: {product['id']}")
                        except Exception as e:
                            connection.rollback()  # Rollback nếu có lỗi với sản phẩm cụ thể
                            print(f"Lỗi khi chèn sản phẩm ID {product['id']}: {e}")

    except Exception as e:
        print(f"Lỗi khi đọc tệp: {e}")
    finally:
        cursor.close()
        connection.close()
        print(f"Tổng số sản phẩm đã chèn: {total_products}")
        print("Đã hoàn thành việc tải dữ liệu vào cơ sở dữ liệu.")

if __name__ == "__main__":
    load_json_files_to_db('output')





'''product_json = [
    {
        "id": 1391347,
        "name": "Tranh xếp Hình Tia Sáng  Gương Thu Tia Sáng (2035 Mảnh Ghép)",
        "url_key": "tranh-xep-hinh-tia-sang-guong-thu-tia-sang-2035-manh-ghep-p1391347",
        "price": 197600,
        "description": "Bộ Xếp Hình Gương Thu Tia Sáng (2035 Mảnh Ghép)được làm từ chất liệu giấy bồi cao cấp...",
        "images_url": [
            "https://salt.tikicdn.com/ts/product/f4/6e/ff/18952da934d8ae12e0bd1edcf0ac2c8f.jpg",
            "https://salt.tikicdn.com/ts/product/2f/f4/53/c7b86df00b395a9c5157e99d266d5e1a.jpg"
        ]
    },
    # Bạn có thể thêm các sản phẩm khác ở đây...
]

connection, cursor = get_db_connection()  # Tách tuple thành connection và cursor

if connection is None or cursor is None:
    print("Không thể kết nối tới cơ sở dữ liệu.")
else:
    try:
        # Thực hiện các thao tác chèn dữ liệu
        product = product_json[0]  # Lấy sản phẩm đầu tiên trong JSON
        insert_product(cursor, product)
        connection.commit()
        print("Chèn sản phẩm thành công!")
    except Exception as e:
        connection.rollback()  # Rollback khi có lỗi
        print("Lỗi khi chèn sản phẩm:", e)
    finally:
        cursor.close()
        connection.close()  # Đóng kết nối sau khi thực hiện xong
'''        