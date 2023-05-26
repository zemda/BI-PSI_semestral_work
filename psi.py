import socket
from concurrent.futures import ThreadPoolExecutor

HOST = '127.0.0.1'
PORT = 6969

TIMEOUT = 1
TIMEOUT_RECHARGING = 5

KEYS = [(23019, 32037), (32037, 29295), (18789, 13603), (16443, 29533), (18189, 21952)]


def handle_client(conn):
    try:
        auth(conn)
        navigate_to_origin(conn)

        # Begin of Secret message discovery
        conn.send(b"105 GET MESSAGE\a\b")
        receive_message(conn, "", max_length=100)
        conn.send(b"106 LOGOUT\a\b")

    except Exception as e:
        if str(e) == 'SERVER_SYNTAX_ERROR':
            conn.send(b"301 SYNTAX ERROR\a\b")
        if str(e) == 'SERVER_LOGIC_ERROR':
            conn.send(b"302 LOGIC ERROR\a\b")
    finally:
        conn.close()


def receive_message(conn, expected_message_type, max_length=None, strip_spaces=True):
    buffer = ""
    is_recharging = False

    while True:
        try:
            char = conn.recv(1).decode()
        except socket.timeout:
            if is_recharging:
                raise Exception("whew")
            return None

        buffer += char
        if max_length > 15 or any(char.isdigit() for char in buffer):  # tests for max length (Optimization)
            if not buffer.endswith("\a\b") and len(buffer) == max_length:
                raise Exception('SERVER_SYNTAX_ERROR')

        if buffer.endswith("RECHARGING\a\b"):
            is_recharging = True
            conn.settimeout(TIMEOUT_RECHARGING)
            buffer = ""
        elif buffer.endswith("FULL POWER\a\b"):
            if not is_recharging:
                raise Exception("SERVER_LOGIC_ERROR")
            conn.settimeout(TIMEOUT)
            is_recharging = False
            buffer = ""
        elif buffer.endswith("\a\b"):
            if is_recharging:
                raise Exception("SERVER_LOGIC_ERROR")
            if max_length and len(buffer) > max_length:
                raise Exception('SERVER_SYNTAX_ERROR')
            if "\a\b" in buffer[:-2]:
                raise Exception('SERVER_SYNTAX_ERROR')

            message = buffer[:-2]
            if message.startswith(expected_message_type):
                if strip_spaces:
                    return message[len(expected_message_type):].strip()
                else:
                    return message[len(expected_message_type):]
            else:
                raise Exception('SERVER_SYNTAX_ERROR')


def execute_move_command(conn):
    conn.send(b"102 MOVE\a\b")
    response_str = receive_message(conn, "OK ", max_length=12, strip_spaces=False)
    response = response_str.strip()
    if response != response_str or response is None:
        raise Exception('SERVER_SYNTAX_ERROR')
    try:
        new_position = tuple(map(int, response.split()))
    except ValueError:
        raise Exception('SERVER_SYNTAX_ERROR')
    return new_position


def turn(orientation, conn, _dir):
    conn.send(b"104 TURN RIGHT\a\b") if _dir == "right" else conn.send(b"103 TURN LEFT\a\b")
    response = receive_message(conn, "OK ", max_length=12)
    position = tuple(map(int, response.split()))
    index = (["n", "e", "s", "w"].index(orientation) + (1 if _dir == "right" else -1)) % 4
    return position, ["n", "e", "s", "w"][index]


def navigate_to_origin(conn):
    position, orientation = determine_initial_position_and_orientation(conn)
    while position != (0, 0):
        for i in range(2):
            direction = ('w' if position[0] > 0 else 'e', 'n' if position[1] < 0 else 's')[i]
            if position[i] != 0:
                while orientation != direction:
                    position, orientation = turn(orientation, conn, "left")

                while position[i] != 0:
                    old_position, position = position, execute_move_command(conn)
                    if old_position == position or position[i] == 0:
                        break
            elif position[1-i] != 0:
                position, orientation = turn(orientation, conn, "left")
                position = execute_move_command(conn)
    return


def determine_initial_position_and_orientation(conn):
    position1, position2 = execute_move_command(conn), execute_move_command(conn)
    if position1 == position2:  # is blocked by obstacle
        conn.send(b"104 TURN RIGHT\a\b")
        receive_message(conn, "OK ", max_length=12)
        position2 = execute_move_command(conn)
    dx, dy = position2[0] - position1[0], position2[1] - position1[1]
    orientation = "e" if dx > 0 else "w" if dx < 0 else "n" if dy > 0 else "s"
    return position2, orientation


def auth(conn):
    conn.settimeout(TIMEOUT)
    username = receive_message(conn, "", max_length=20)
    if not username:
        raise Exception()

    conn.send(b"107 KEY REQUEST\a\b")
    key_id_str = receive_message(conn, "", max_length=5)
    try:
        key_id = int(key_id_str)
    except Exception:
        raise Exception('SERVER_SYNTAX_ERROR')

    if key_id < 0 or key_id > 4:
        conn.send(b"303 KEY OUT OF RANGE\a\b")
        raise Exception()

    server_key = KEYS[key_id][0]
    client_key = KEYS[key_id][1]

    username_hash = sum(b for b in username.encode()) * 1000 % 65536
    server_confirmation = (username_hash + server_key) % 65536
    conn.send(f"{server_confirmation}\a\b".encode())

    client_confirmation_str = receive_message(conn, "", max_length=7, strip_spaces=False)
    if client_confirmation_str is None:
        raise ConnectionError("Connection timed out while receiving client confirmation")

    if client_confirmation_str != client_confirmation_str.strip():
        raise Exception('SERVER_SYNTAX_ERROR')

    try:
        client_confirmation = int(client_confirmation_str)
    except Exception:
        raise Exception('SERVER_SYNTAX_ERROR')

    expected_client_confirmation = (username_hash + client_key) % 65536
    if client_confirmation != expected_client_confirmation:
        conn.send(b"300 LOGIN FAILED\a\b")
        raise Exception()

    conn.send(b"200 OK\a\b")


if __name__ == "__main__":
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()

        print(f"Server started on {HOST}:{PORT}")
        with ThreadPoolExecutor(max_workers=4) as executor:
            while True:
                conn, addr = s.accept()
                executor.submit(handle_client, conn)
