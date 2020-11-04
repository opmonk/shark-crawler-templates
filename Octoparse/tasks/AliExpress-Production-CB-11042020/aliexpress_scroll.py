import pyautogui
import time

print('Press Ctrl-C to quit.')

x, y = pyautogui.position()
try:
    while True:
        pyautogui.moveTo(x - 20 , y + 20)
        pyautogui.vscroll(500)
        time.sleep(0.1)
        pyautogui.moveTo(x + 20 , y - 20)

except KeyboardInterrupt:
    print('\n')
	
