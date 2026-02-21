"use client";
import React from 'react';

// Curation Seed Data - ME/CFS & Long COVID Focus
const mockCases = [
  {
    id: "case_114",
    condition: "Myalgic Encephalomyelitis (ME/CFS)",
    onset: "Year 3",
    threatToPersonhood: "The Loss of Unplanned Time",
    description: "Before people lose the ability to work or walk, they lose the ability to say 'yes' in the moment. Every action becomes a transaction against an invisible ledger.",
    narrativeFragment: "I used to just leave the house. Now, going to the grocery store requires a three-day calculus of what my spine will tolerate and what I'll have to cancel tomorrow. The spontaneity that defines being alive is gone.",
    compensatoryRituals: "Micro-pacing (breaking showers into 3 seated segments), sensory deprivation in dark rooms for 48 hours pre-event.",
    sourceType: "Public Forum (Anonymized)"
  },
  {
    id: "case_042",
    condition: "Long COVID / Dysautonomia",
    onset: "Year 5",
    threatToPersonhood: "The Erosion of Epistemic Trust",
    description: "Between years 2 and 4, there is a distinct shift from 'the doctors will fix this' to 'the doctors think I am crazy.' The loss is the baseline trust in institutions.",
    narrativeFragment: "You stop bringing up the sharpest pains because if you list too many symptoms, they write 'anxiety' on your chart and the appointment is over. I now go into clinics like I'm preparing for a hostile deposition.",
    compensatoryRituals: "Bringing binders of printed NIH research, having specific articulate friends act as 'medical proxies' because doctors listen to healthy people.",
    sourceType: "Interview Transcript (Paraphrased)"
  },
  {
    id: "case_209",
    condition: "Undiagnosed Chronic Pain / Suspected RA",
    onset: "Year 6",
    threatToPersonhood: "Ambiguous Grief (The Ghost Self)",
    description: "By year 5, a profound, unmournable grief sets in. There is no 'new normal' to adjust to, and society offers no ritual for this kind of loss.",
    narrativeFragment: "My husband is still waiting for the woman he married to come back. I don't know how to tell him she died four years ago in a rheumatologist's waiting room. The physical pain is 7/10; the loneliness of the ghost self is 10/10.",
    compensatoryRituals: "Aggressive masking during short social interactions followed by severe 'crash' periods masked as migraines.",
    sourceType: "NORD Public Patient Story (Adapted)"
  }
];

export default function TheWitnessArchive () {
  return (
    <main style={ { padding: 'var(--space-8)', maxWidth: '1200px', margin: '0 auto', minHeight: '100vh', display: 'flex', flexDirection: 'column' } }>

      {/* Header Section - Solemn and Respectful */ }
      <header className="animate-fade-in" style={ { marginBottom: 'var(--space-12)', textAlign: 'center', marginTop: 'var(--space-6)' } }>
        <div style={ { display: 'flex', justifyContent: 'center', marginBottom: '1rem' } }>
          <div className="pulse-indicator" style={ { marginRight: '8px', marginTop: '10px' } }></div>
        </div>
        <h1 style={ { fontSize: '2.5rem', marginBottom: 'var(--space-4)', color: '#fff', fontWeight: 400, letterSpacing: '0.05em', textTransform: 'uppercase' } }>
          The Witness Archive
        </h1>
        <p style={ { color: 'var(--text-secondary)', fontSize: '1.1rem', maxWidth: '650px', margin: '0 auto', lineHeight: '1.8' } }>
          Preserving the unvarnished texture of lived suffering. <br />
          We index real patient testimony—the threats to personhood, the diagnostic odysseys, the deep friction of chronic illness—so interventions can be tested against ground truth, not abstractions.
        </p>

        {/* Semantic Search Placeholder */ }
        <div style={ { marginTop: 'var(--space-8)', position: 'relative', maxWidth: '700px', margin: 'var(--space-8) auto 0' } }>
          <input
            type="text"
            placeholder="Search the archive (e.g., 'What do people with ME/CFS lose first?')"
            style={ {
              width: '100%', padding: '1.2rem 1.5rem', borderRadius: 'var(--radius-sm)',
              background: 'rgba(255,255,255,0.02)', border: '1px solid var(--border-color)',
              color: 'var(--text-primary)', fontSize: '1rem', outline: 'none', transition: 'border-color 0.4s var(--ease-solemn)'
            } }
            onFocus={ (e) => e.target.style.borderColor = 'rgba(139, 92, 246, 0.4)' }
            onBlur={ (e) => e.target.style.borderColor = 'var(--border-color)' }
          />
          <button className="btn btn-primary" style={ { position: 'absolute', right: '10px', top: '10px', bottom: '10px', padding: '0 1.5rem' } }>
            Query Corpus
          </button>
        </div>
      </header>

      {/* Corpus Density Stats */ }
      <div className="glass-panel animate-fade-in" style={ { display: 'flex', justifyContent: 'space-around', padding: 'var(--space-6)', marginBottom: 'var(--space-12)', animationDelay: '0.4s' } }>
        <div style={ { textAlign: 'center' } }>
          <h3 style={ { fontSize: '1.5rem', color: '#fff', fontWeight: 400 } }>8,402</h3>
          <span className="text-meta">Indexed Testimonies</span>
        </div>
        <div style={ { textAlign: 'center' } }>
          <h3 style={ { fontSize: '1.5rem', color: '#fff', fontWeight: 400 } }>142</h3>
          <span className="text-meta">Mapped Constellations</span>
        </div>
        <div style={ { textAlign: 'center' } }>
          <h3 style={ { fontSize: '1.5rem', color: 'var(--text-secondary)', fontWeight: 400 } }>ME/CFS & Long COVID</h3>
          <span className="text-meta">Active Seed Domain</span>
        </div>
      </div>

      {/* The Archive Wall - Journey Cards */ }
      <section style={ { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: 'var(--space-8)', paddingBottom: 'var(--space-12)' } }>
        { mockCases.map((c, i) => (
          <article key={ c.id } className="glass-panel interactive-card animate-fade-in" style={ { padding: 'var(--space-8)', animationDelay: `${0.8 + (i * 0.3)}s`, display: 'flex', flexDirection: 'column' } }>

            <header style={ { marginBottom: 'var(--space-6)', borderBottom: '1px solid var(--border-color)', paddingBottom: 'var(--space-4)' } }>
              <div style={ { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '12px' } }>
                <span className="badge">{ c.condition }</span>
                <span className="text-meta" style={ { color: 'var(--text-tertiary)' } }>{ c.onset }</span>
              </div>
              <h2 style={ { fontSize: '1.2rem', margin: '0 0 8px 0', color: '#fff', fontWeight: 400 } }>
                Subject { c.id.split('_')[1] }
              </h2>
              <div style={ { display: 'flex', alignItems: 'center', gap: '8px' } }>
                <div className="pulse-indicator" style={ { backgroundColor: 'var(--accent-secondary)', width: '4px', height: '4px', boxShadow: 'none', animation: 'none' } }></div>
                <h3 style={ { fontSize: '0.9rem', color: 'var(--accent-secondary)', fontWeight: 400, letterSpacing: '0.02em', margin: 0 } }>
                  { c.threatToPersonhood }
                </h3>
              </div>
            </header>

            <div style={ { display: 'flex', flexDirection: 'column', gap: 'var(--space-6)', flexGrow: 1 } }>
              <div>
                <p style={ { fontSize: '0.9rem', color: 'var(--text-secondary)', lineHeight: '1.6', marginBottom: '12px' } }>
                  { c.description }
                </p>
                <div className="text-quote">
                  "{ c.narrativeFragment }"
                </div>
              </div>

              <div style={ { marginTop: 'auto', paddingTop: 'var(--space-4)' } }>
                <h4 className="text-meta" style={ { marginBottom: '8px' } }>Compensatory Rituals</h4>
                <p style={ { fontSize: '0.9rem', color: '#94a3b8' } }>
                  { c.compensatoryRituals }
                </p>
              </div>
            </div>

            <footer style={ { marginTop: 'var(--space-6)', paddingTop: 'var(--space-4)', borderTop: '1px solid var(--border-color)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' } }>
              <span style={ { fontSize: '0.75rem', color: 'var(--text-tertiary)' } }>
                Source: { c.sourceType }
              </span>
              <button className="btn" style={ { padding: '4px 12px', fontSize: '0.8rem', background: 'transparent', color: 'var(--text-secondary)' } }>
                Expand Node &rarr;
              </button>
            </footer>

          </article>
        )) }
      </section>

    </main>
  );
}
