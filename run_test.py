import asyncio
import websockets
import json


async def listen():
    #uri = "ws://127.0.0.1:8000/ws"
    uri = "wss://coding-checker-model-d3abhrd2fcg6dghw.japaneast-01.azurewebsites.net/ws"  # Change this to your server's address if needed
    #   # Change this to your server's address if needed
    
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            print(f"Connection attempt {attempt + 1}/{max_retries}...")
            
            # Configure connection with optimized timeout settings
            async with websockets.connect(
                uri, 
                open_timeout=45,  # Increase timeout to 45 seconds for initial connection
                close_timeout=15,
                ping_interval=15,  # Send ping every 15 seconds
                ping_timeout=10    # Wait 10 seconds for pong
            ) as websocket:
                print("✓ Connected to WebSocket server successfully")

                try:
                    while True:
                        message = await asyncio.wait_for(websocket.recv(), timeout=120)
                        
                        # Try to parse as JSON to handle keepalive messages
                        try:
                            msg_data = json.loads(message)
                            if msg_data.get("type") == "keepalive":
                                print("[Keepalive] Connection active")
                                continue
                        except json.JSONDecodeError:
                            pass  # Not JSON, treat as regular message
                        
                        print(f"Received: {message}")
                        
                except asyncio.TimeoutError:
                    print("No message received for 2 minutes. Connection may have timed out.")
                    break
                except websockets.exceptions.ConnectionClosed:
                    print("Connection closed by server")
                    break
                    
                # If we reach here, connection was successful
                return
        
        except asyncio.TimeoutError:
            print(f"❌ Connection attempt {attempt + 1} timed out.")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                print("All connection attempts failed. Server might be busy with long-running operations.")
                print("Try running the script again in a few moments.")
        except websockets.exceptions.WebSocketException as e:
            print(f"❌ WebSocket error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                print("All connection attempts failed due to WebSocket errors.")
        except Exception as e:
            print(f"❌ Unexpected error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                print("All connection attempts failed due to unexpected errors.")


if __name__ == "__main__":
    asyncio.run(listen())
