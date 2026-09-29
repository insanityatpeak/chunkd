import { useEffect, useState } from 'preact/hooks';

const STEPS = [
  { title: 'Upload a file', text: 'Use "Upload a file" below. The client splits it into 4 MiB chunks named by their SHA-256 and sends each to 3 nodes in parallel.' },
  { title: 'Look at the placement', text: 'Click the file name. The grid shows which nodes hold each chunk: 3 copies, on different racks when there are enough racks.' },
  { title: 'Kill a node', text: 'In "Break it", kill a node that holds your chunks. Its card turns amber (suspect) after 3 s and red (dead) after 10 s of silence.' },
  { title: 'Watch the repair', text: 'After a 20 s delay (in case the node is only rebooting), the timeline shows copies from surviving replicas, at most 8 at a time. Speed up with 10× or 50×.' },
  { title: 'Download and verify', text: 'Download the file. Your browser checks every chunk and the whole file against their SHA-256; a node that served bad bytes would be skipped and reported.' },
];

// Tour is a five-step overlay; dismissing it is remembered in this browser.
export function Tour({ onClose }: { onClose(): void }) {
  const [i, setI] = useState(0);
  const close = () => {
    try {
      localStorage.setItem('chunkd.tour', 'done');
    } catch {
      // Private mode: the tour just shows again next time.
    }
    onClose();
  };
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && close();
    addEventListener('keydown', onKey);
    return () => removeEventListener('keydown', onKey);
  });
  const s = STEPS[i];
  return (
    <div class="tour" role="dialog" aria-modal="false" aria-labelledby="tour-title">
      <p class="muted small">
        Step {i + 1} of {STEPS.length}
      </p>
      <h2 id="tour-title">{s.title}</h2>
      <p>{s.text}</p>
      <div class="tour-actions">
        <button type="button" onClick={close}>
          Close
        </button>
        <button type="button" disabled={i === 0} onClick={() => setI(i - 1)}>
          Back
        </button>
        {i < STEPS.length - 1 ? (
          <button type="button" class="primary" onClick={() => setI(i + 1)}>
            Next
          </button>
        ) : (
          <button type="button" class="primary" onClick={close}>
            Done
          </button>
        )}
      </div>
    </div>
  );
}

// tourSeen reports whether this browser already dismissed the tour.
export function tourSeen(): boolean {
  try {
    return localStorage.getItem('chunkd.tour') === 'done';
  } catch {
    return false;
  }
}
