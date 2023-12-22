# Databricks notebook source
print("Entering notebook 2")

# COMMAND ----------

# Binary search function
def binary_search(arr, low, high, x):
    
    # If the array is empty
    if high < low:
        return -1
    
    # Calculate the midpoint index
    mid = (low + high) // 2
    
    # If the element is present at the midpoint
    if arr[mid] == x:
        return mid
    
    # If the element is smaller than the midpoint, ignore the right half
    elif arr[mid] > x:
        return binary_search(arr, low, mid - 1, x)
    
    # If the element is larger than the midpoint, ignore the left half
    else:
        return binary_search(arr, mid + 1, high, x)
        

# Example array
arr = [1, 2, 3, 4, 5, 6, 7, 8, 9]
x = 5

# Call the binary_search function
result = binary_search(arr, 0, len(arr) - 1, x)

if result != -1:
    print(f"Element is present at index {result}")
else:
    print("Element is not present in array")


# COMMAND ----------

print("Exiting notebook 2 now!!!")
