# VHS Tape for cbio demo
Output docs/img/tui_demo.gif

Set Shell "bash"
Set FontSize 16
Set Width 1200
Set Height 800
Set Padding 20
Set Theme "Catppuccin Mocha"
Set TypingSpeed 50ms

# --- HIDE STARTUP ---
Hide
Type "uv run cbio" Enter
# Wait for the app to enter full-screen mode completely
Show
# --- END HIDE ---

# 1. Search for a study
Sleep 4s
Type "search brca"
Sleep 500ms
Enter
Sleep 3s

# 2. Navigate and Select
Down 5
Sleep 500ms
Enter
Sleep 1s

# 3. Select Mutations
Enter
Sleep 1s

# 4. Select MAF
Enter
Sleep 1s

# 5. Confirm filename
#Type "brca_results.maf"
Sleep 500ms
Enter
Sleep 4s

# 6. Exit using Double Ctrl+D
Ctrl+D
Sleep 500ms
Ctrl+D
Sleep 1s
