# The README's demo GIF: a real compose cluster losing a node and repairing
# itself. Render with: go run ./tools/task gif
# (runs ghcr.io/charmbracelet/vhs with the repo at /vhs, a Linux build of
# chunkd-demo in /vhs/bin, and the Docker socket, against localhost:8080).

Output docs/assets/demo.gif

Set Shell bash
Set FontSize 15
Set Width 1060
Set Height 700
Set Padding 18
Set Framerate 12
Set TypingSpeed 35ms
Set Theme "Catppuccin Mocha"

Hide
Type "export PATH=/vhs/bin:$PATH CHUNKD_GATEWAY=http://host.docker.internal:8080 && clear"
Enter
Sleep 500ms
Show

Type "# a real 9-container cluster: 3 metadata servers, 5 storage nodes, 1 gateway"
Enter
Type "# chunkd-demo is what 'docker compose run --rm demo' runs"
Enter
Sleep 800ms
Type "chunkd-demo"
Enter
Wait@120s
Sleep 4s
