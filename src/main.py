import json
import asyncio
import aiohttp
import aiofiles
import os
import logging
import sys

# Настройка логгера
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DispatchLandParser:
    def __init__(self, url, token, start_page, page_count, cookies, concurrency_limit=3):
        self.url = url
        self.token = token
        self.start_page = start_page
        self.page_count = page_count
        self.cookies = cookies
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en',
            'Authorization': f'Bearer {self.token}',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0.1 Safari/605.1.15'
        }
        self.semaphore = asyncio.Semaphore(concurrency_limit)

    def get_pages_folder(self):
        return "pages"

    def get_load_details_folder(self):
        return "load_details"

    def get_travel_order_folder(self):
        return "travel_order_details"

    def get_truck_folder(self):
        return "truck_details"

    def get_owner_folder(self):
        return "owner_details"

    def get_customer_folder(self):
        return "customer_details"

    # Проверка статуса ответа и остановка при критических ошибках
    async def _check_response_status(self, response):
        if response.status >= 500:
            logger.error(f"Критическая ошибка сервера {response.status}: {response.reason}")
            await asyncio.sleep(30)
            # sys.exit(f"Серверная ошибка {response.status}. Парсер остановлен.")
        elif response.status in [401, 403]:
            logger.error(f"Ошибка авторизации {response.status}: {response.reason}")
            sys.exit(f"Ошибка авторизации {response.status}. Парсер остановлен.")
        elif response.status != 200:
            logger.warning(f"Неуспешный статус ответа {response.status}: {response.reason}")

    async def _post_request(self, session, endpoint, data):
        async with self.semaphore:
            try:
                async with session.post(self.url + endpoint, headers=self.headers, cookies=self.cookies, json=data) as response:
                    await self._check_response_status(response)
                    if response.status == 200:
                        logger.info(f"POST запрос к {endpoint} успешен.")
                        return await response.json()
                    return None
            except Exception as e:
                logger.exception(f"Ошибка при выполнении POST запроса к {endpoint}: {e}")
                return None

    async def _get_request(self, session, endpoint):
        async with self.semaphore:
            try:
                async with session.get(self.url + endpoint, headers=self.headers, cookies=self.cookies) as response:
                    await self._check_response_status(response)
                    if response.status == 200:
                        logger.info(f"GET запрос к {endpoint} успешен.")
                        return await response.json()
                    return None
            except Exception as e:
                logger.exception(f"Ошибка при выполнении GET запроса к {endpoint}: {e}")
                return None

    async def save_json_to_file(self, data, file_name, folder_path):
        os.makedirs(folder_path, exist_ok=True)
        full_path = os.path.join(folder_path, file_name)
        if not os.path.exists(full_path):
            async with aiofiles.open(full_path, 'w') as f:
                await f.write(json.dumps(data, indent=4))
            logger.info(f"Данные сохранены в {full_path}")
        else:
            logger.info(f"Файл {file_name} уже существует, пропускаем сохранение.")

    async def parse_pages(self, session):
        folder_path = self.get_pages_folder()
        tasks = []
        for page_number in range(self.start_page, self.page_count + 1):
            file_name = f'sp_loads_page_{page_number}.json'
            full_path = os.path.join(folder_path, file_name)

            if os.path.exists(full_path):
                logger.info(f"Файл {file_name} уже существует, пропускаем запрос.")
                continue

            data = {
                "page": page_number,
                "perPage": 500,
                "sortBy": {
                    "lastDelivery": "asc"
                },
                # "truckNumber": "MCOT2557",
                "loadStatus": [
                    "Completed"
                ]
            }
            tasks.append(self._parse_page_task(session, data, file_name, folder_path))

        await asyncio.gather(*tasks)

    async def _parse_page_task(self, session, data, file_name, folder_path):
        json_data = await self._post_request(session, '/api/sp-loads', data)
        if json_data:
            await self.save_json_to_file(json_data, file_name, folder_path)

    async def fetch_load_details(self, session):
        pages_folder = self.get_pages_folder()
        output_folder = self.get_load_details_folder()
        tasks = []
        for file_name in os.listdir(pages_folder):
            if file_name.endswith('.json'):
                tasks.append(self._fetch_load_task(session, file_name, pages_folder, output_folder))

        await asyncio.gather(*tasks)

    async def _fetch_load_task(self, session, file_name, pages_folder, output_folder):
        full_path = os.path.join(pages_folder, file_name)
        async with aiofiles.open(full_path, 'r') as f:
            data = json.loads(await f.read())

        for item in data:
            load_number = item.get("number")
            if load_number:
                output_file_name = f'load_{load_number}.json'
                full_output_path = os.path.join(output_folder, output_file_name)

                if os.path.exists(full_output_path):
                    logger.info(f"Файл {output_file_name} уже существует, пропускаем запрос.")
                    continue

                endpoint = f'/api/sp-loads/{load_number}'
                load_details = await self._get_request(session, endpoint)
                if load_details:
                    await self.save_json_to_file(load_details, output_file_name, output_folder)

    async def fetch_travel_order_details(self, session):
        pages_folder = self.get_pages_folder()
        travel_order_folder = self.get_travel_order_folder()
        tasks = []
        for file_name in os.listdir(pages_folder):
            if file_name.endswith('.json'):
                tasks.append(self._fetch_travel_order_task(session, file_name, pages_folder, travel_order_folder))

        await asyncio.gather(*tasks)

    async def _fetch_travel_order_task(self, session, file_name, pages_folder, travel_order_folder):
        full_path = os.path.join(pages_folder, file_name)
        async with aiofiles.open(full_path, 'r') as f:
            data = json.loads(await f.read())

        for item in data:
            travel_orders = item.get("travelOrders", [])
            for travel_order in travel_orders:
                travel_order_number = travel_order.get("number", [])
                if travel_order_number:
                    output_file_name = f'travel_order_{travel_order_number}.json'
                    full_output_path = os.path.join(travel_order_folder, output_file_name)

                    if os.path.exists(full_output_path):
                        logger.info(f"Файл {output_file_name} уже существует, пропускаем запрос.")
                        continue

                    endpoint = f'/api/travel-order/{travel_order_number}'
                    travel_order_details = await self._get_request(session, endpoint)
                    if travel_order_details:
                        await self.save_json_to_file(travel_order_details, output_file_name, travel_order_folder)

    async def fetch_truck_details(self, session):
        pages_folder = self.get_pages_folder()
        truck_folder = self.get_truck_folder()
        tasks = []
        for file_name in os.listdir(pages_folder):
            if file_name.endswith('.json'):
                tasks.append(self._fetch_truck_task(session, file_name, pages_folder, truck_folder))

        await asyncio.gather(*tasks)

    async def _fetch_truck_task(self, session, file_name, load_details_folder, truck_folder):
        full_path = os.path.join(load_details_folder, file_name)
        async with aiofiles.open(full_path, 'r') as f:
            data = json.loads(await f.read())

        for item in data:
            travel_orders = item.get("travelOrders", [])
            if travel_orders:
                truck = travel_orders[0].get("truck")
                if truck and truck.get("number"):
                    truck_number = truck["number"]
                    output_file_name = f'truck_{truck_number}.json'
                    full_output_path = os.path.join(truck_folder, output_file_name)

                    if os.path.exists(full_output_path):
                        logger.info(f"Файл {output_file_name} уже существует, пропускаем запрос.")
                        return

                    endpoint = f'/api/trucks/search/{truck_number}'
                    truck_details = await self._get_request(session, endpoint)
                    if truck_details:
                        await self.save_json_to_file(truck_details, output_file_name, truck_folder)

    async def run_all_tasks(self):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            logger.info("Начало парсинга страниц.")
            await self.parse_pages(session)
            logger.info("Парсинг страниц завершен.")

            logger.info("Начало загрузки деталей грузов.")
            await self.fetch_load_details(session)
            logger.info("Загрузка деталей грузов завершена.")

            logger.info("Начало загрузки деталей travel orders.")
            await self.fetch_travel_order_details(session)
            logger.info("Загрузка деталей travel orders завершена.")

            logger.info("Начало загрузки деталей trucks.")
            await self.fetch_truck_details(session)
            logger.info("Загрузка деталей trucks завершена.")


if __name__ == "__main__":
    url = "https://ot.dispatchland.com"
    token = "OTJhMDIxM2YwMjk2ODliMGRjODliZjE0NmUxNTJkNzljNjFlNzg2NmY5M2EwNjczMTAwNzU1NzM4ZDI3ODVjMg"
    start_page = 1  # Начальная страница
    page_count = 4  # Количество страниц для обработки
    cookies = {
        "AWSALB": "78DX7XAFPPlhDH84XmI5DpyiBwoa9HaDXS6pac5Eywlx/9KVNWynJ2/L8yeQNnYLOACRT102+DCuhIbmdiYUoHuIg6m3j0UdUGi47CYb/Moss6pnAKn1aVkXNw2T",
        "AWSALBCORS": "78DX7XAFPPlhDH84XmI5DpyiBwoa9HaDXS6pac5Eywlx/9KVNWynJ2/L8yeQNnYLOACRT102+DCuhIbmdiYUoHuIg6m3j0UdUGi47CYb/Moss6pnAKn1aVkXNw2T",
        "connect.sid": "s%3AULRvnTu11bLW8hgrBMyBnqUdhf3iJWVo.64N0Hkn9OLwhO7i6iYryyy1pfpERdixaK4KTQgZaNcY",
        "pechenka": "OTJhMDIxM2YwMjk2ODliMGRjODliZjE0NmUxNTJkNzljNjFlNzg2NmY5M2EwNjczMTAwNzU1NzM4ZDI3ODVjMg"
    }
    concurrency_limit = 1

    parser = DispatchLandParser(url, token, start_page, page_count, cookies, concurrency_limit)
    asyncio.run(parser.run_all_tasks())
