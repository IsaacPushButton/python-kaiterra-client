import historypull
import profile as cp

print("Running historypuller")
stats = cp.run('historypull.pull_history()')
print("Program complete")
print(stats)
      
