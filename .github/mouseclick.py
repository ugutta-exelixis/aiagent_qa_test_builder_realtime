import pyautogui
import time

try:
    while True:
        pyautogui.click()
        time.sleep(3)
except pyautogui.FailSafeException:
    print("Stopped: mouse moved to a screen corner.")