# Python code to implement the approach

# Function to return max sum such that
# no two elements are adjacent
def findMaxSum(arr, n):
    incl = 0
    excl = 0
    for i in arr:
        # Current max excluding i
        new_excl = max(excl, incl)

        # Current max including i
        incl = excl + i
        excl = new_excl

    # Return max of incl and excl
    return max(excl, incl)

#
# Driver code
if __name__ == "__main__":
    arr = [2,5,6,1,5,10,6]
    N = 6

    # Function call
    print(findMaxSum(arr, N))


