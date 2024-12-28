import requests
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
from requests.exceptions import Timeout, RequestException, HTTPError
from tenacity import retry, stop_after_attempt, wait_exponential

API_URL = "https://api.tiki.vn/product-detail/api/v1/products/"

# Retry khi gặp lỗi 
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
def fetch_product_data(product_id):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    url = f"{API_URL}{product_id}"

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # Kiểm tra mã trạng thái HTTP, ném ngoại lệ nếu lỗi

        # Nếu mã trạng thái là 404 bỏ qua không retry
        if response.status_code == 404:
            print(f"Sản phẩm với ID {product_id} không tồn tại (404).")
            return None

        data = response.json()

        # Kiểm tra xem dữ liệu có đầy đủ không
        if "description" not in data or "id" not in data:
            print(f"Dữ liệu không đầy đủ cho sản phẩm {product_id}.")
            return None

        # Chuẩn hóa nội dung của "description" bằng cách loại bỏ HTML tags
        description = data.get("description", "")
        soup = BeautifulSoup(description, "html.parser")
        normalized_description = soup.get_text(strip=True)

        # Lấy thông tintin
        product_info = {
            "id": data.get("id"),
            "name": data.get("name"),
            "url_key": data.get("url_key"),
            "price": data.get("price"),
            "description": normalized_description,
            "images_url": [img.get("base_url") for img in data.get("images", []) if img.get("base_url")],
        }
        return product_info

    except Timeout:
        print(f"Timeout khi tải sản phẩm {product_id}.")
        return None  # Bỏ qua và không retry

    except RequestException as e:
        print(f"Lỗi khi tải sản phẩm {product_id}: {e}")
        return None  # Bỏ qua và không retry 

    except HTTPError as e:
        print(f"Lỗi HTTP khi tải sản phẩm {product_id}: {e}")
        if response.status_code in [500, 503]:
            print(f"Lỗi server (500/503) khi tải sản phẩm {product_id}, không retry.")
            return None  
        return None  # Bỏ qua cho các lỗi HTTP khác

    except Exception as e:
        print(f"Lỗi không xác định khi tải sản phẩm {product_id}: {e}")
        return None  # Bỏ qua và không retry


def fetch_all_products(product_ids, max_workers=10):
    products_data = []
    failed_product_ids = []  # Lưu lại các ID bị lỗi 404 hoặc không retry
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(fetch_product_data, product_ids))
        for result, product_id in zip(results, product_ids):
            if result:
                products_data.append(result)  
            else:
                failed_product_ids.append(product_id)  # Lưu các ID lỗi

    if failed_product_ids:
        print(f"Các sản phẩm không tải được hoặc bị lỗi: {failed_product_ids}")
        
    return products_data

def save_to_json(data, output_dir, batch_size=1000):
    if not data:
        print("Không có dữ liệu hợp lệ để lưu!")
        return
    Path(output_dir).mkdir(parents=True, exist_ok=True) 
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        file_path = Path(output_dir) / f"products_{i // batch_size + 1}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(batch, f, ensure_ascii=False, indent=4)
        print(f"Đã lưu {len(batch)} sản phẩm vào {file_path}")


def main():
    product_ids_file = "products_id.csv"

    try:
        product_ids = pd.read_csv(product_ids_file)['id'].tolist()
        print(f"Đang tải danh sách {len(product_ids)} sản phẩm từ {product_ids_file}...")

        # Tải dữ liệu sản phẩm
        products_data = fetch_all_products(product_ids)

        # Lưu dữ liệu vào file JSON
        save_to_json(products_data, output_dir="output")

        print("Chương trình đã hoàn thành.")
    except FileNotFoundError:
        print(f"Lỗi: File {product_ids_file} không tồn tại.")
    except KeyError:
        print(f"Lỗi: File {product_ids_file} không chứa cột 'id'.")
    except Exception as e:
        print(f"Lỗi không xác định: {e}")


if __name__ == "__main__":
    main()
