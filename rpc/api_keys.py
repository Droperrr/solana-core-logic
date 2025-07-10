api_keys = [
    "bbd7a048-29c1-4020-aad9-158c462812da",
    "8b65d9ce-4f9b-4b90-9e4c-af088de240b2",  # второй ключ для теста ротации
    # добавь сюда другие ключи при необходимости
]

current_key_index = 0

def get_current_api_key():
    return api_keys[current_key_index]

def rotate_api_key():
    global current_key_index
    current_key_index = (current_key_index + 1) % len(api_keys)
    return get_current_api_key()

def get_all_api_keys():
    return api_keys
