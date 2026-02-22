import React from 'react';
import { getAllCases } from '@/lib/data';
import { Activity, Database, AlertTriangle, Fingerprint, BookOpen, Clock, Globe } from 'lucide-react';

export default function TheWitnessArchive () {
  const cases = getAllCases();

  // Count unique threat_to_personhood constellations
  const constellations = new Set(cases.map(c => c.threat_to_personhood)).size;

  return (
    <main style={ { padding: 'var(--space-8)', maxWidth: '1400px', margin: '0 auto', minHeight: '100vh', display: 'flex', flexDirection: 'column' } }>

      {/* Header Section - Solemn and Respectful */ }
      <header className="animate-fade-in" style={ { marginBottom: 'var(--space-12)', textAlign: 'center', marginTop: 'var(--space-6)' } }>
        <div style={ { display: 'flex', justifyContent: 'center', marginBottom: '1.5rem' } }>
          <div style={ { padding: '12px', background: 'rgba(139, 92, 246, 0.05)', borderRadius: '50%', border: '1px solid rgba(139, 92, 246, 0.2)' } }>
            <Database size={ 32 } color="var(--accent-primary)" />
          </div>
        </div>
        <h1 style={ { fontSize: '3.5rem', marginBottom: 'var(--space-4)', color: '#fff', fontWeight: 300, letterSpacing: '0.08em', textTransform: 'uppercase' } }>
          Sufferpedia Explorer
        </h1>
        <p style={ { color: 'var(--text-secondary)', fontSize: '1.2rem', maxWidth: '750px', margin: '0 auto', lineHeight: '1.8' } }>
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
              color: 'var(--text-primary)', fontSize: '1rem', outline: 'none', transition: 'border-color 0.4s var(--ease-solemn)',
              boxShadow: '0 4px 20px rgba(0,0,0,0.2)'
            } }
          />
          <button className="btn btn-primary" style={ { position: 'absolute', right: '10px', top: '10px', bottom: '10px', padding: '0 1.5rem' } }>
            Query Corpus
          </button>
        </div>
      </header>

      {/* Corpus Density Stats */ }
      <div className="glass-panel animate-fade-in" style={ { display: 'flex', justifyContent: 'space-around', padding: 'var(--space-6)', marginBottom: 'var(--space-12)', animationDelay: '0.4s' } }>
        <div style={ { textAlign: 'center', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '8px' } }>
          <BookOpen size={ 24 } color="var(--text-tertiary)" />
          <h3 style={ { fontSize: '1.8rem', color: '#fff', fontWeight: 400 } }>{ cases.length }</h3>
          <span className="text-meta">Indexed Testimonies</span>
        </div>
        <div style={ { textAlign: 'center', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '8px' } }>
          <Activity size={ 24 } color="var(--text-tertiary)" />
          <h3 style={ { fontSize: '1.8rem', color: '#fff', fontWeight: 400 } }>{ constellations }</h3>
          <span className="text-meta">Distinct Constellations</span>
        </div>
        <div style={ { textAlign: 'center', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '8px' } }>
          <Globe size={ 24 } color="var(--text-tertiary)" />
          <h3 style={ { fontSize: '1.8rem', color: 'var(--text-secondary)', fontWeight: 400 } }>Multiple Domains</h3>
          <span className="text-meta">Active Web Crawlers</span>
        </div>
      </div>

      {/* The Archive Wall - Journey Cards */ }
      <section style={ { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(400px, 1fr))', gap: 'var(--space-8)', paddingBottom: 'var(--space-12)' } }>
        { cases.length === 0 && (
          <article className="glass-panel" style={ { padding: 'var(--space-8)' } }>
            <h2 style={ { fontSize: '1.2rem', marginBottom: '8px' } }>No testimonies indexed yet</h2>
            <p style={ { color: 'var(--text-secondary)', lineHeight: '1.7' } }>
              Run the crawl and parse pipeline to generate JSON files in <code>public_data/processed</code>.
            </p>
          </article>
        ) }
        { cases.map((c, i) => (
          <article key={ c.id } className="glass-panel interactive-card animate-fade-in" style={ { padding: 'var(--space-8)', animationDelay: `${0.8 + (Math.min(i, 8) * 0.1)}s`, display: 'flex', flexDirection: 'column' } }>

            <header style={ { marginBottom: 'var(--space-6)', borderBottom: '1px solid var(--border-color)', paddingBottom: 'var(--space-4)' } }>
              <div style={ { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '16px' } }>
                <span className="badge" style={ { background: 'rgba(255,255,255,0.05)', color: '#fff', border: '1px solid rgba(255,255,255,0.1)' } }>{ c.condition }</span>
                <span className="text-meta" style={ { color: 'var(--text-tertiary)', display: 'flex', alignItems: 'center', gap: '4px' } }><Clock size={ 12 } /> { c.onset }</span>
              </div>
              <h2 style={ { fontSize: '1.4rem', margin: '0 0 12px 0', color: '#fff', fontWeight: 300, letterSpacing: '0.02em', display: 'flex', alignItems: 'center', gap: '8px' } }>
                <Fingerprint size={ 18 } color="var(--text-tertiary)" />
                Subject { c.id.split('_')[1] }
              </h2>
              <div style={ { display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap', background: 'rgba(139, 92, 246, 0.05)', padding: '8px 12px', borderRadius: '4px', borderLeft: '2px solid var(--accent-primary)' } }>
                <h3 style={ { fontSize: '0.95rem', color: 'var(--text-primary)', fontWeight: 500, letterSpacing: '0.02em', margin: 0, lineHeight: '1.4' } }>
                  { c.threat_to_personhood }
                </h3>
              </div>
            </header>

            <div style={ { display: 'flex', flexDirection: 'column', gap: 'var(--space-6)', flexGrow: 1 } }>
              <div>
                <p style={ { fontSize: '0.95rem', color: 'var(--text-secondary)', lineHeight: '1.7', marginBottom: '16px' } }>
                  { c.description }
                </p>
                <div className="text-quote" style={ { fontSize: '1.1rem', fontStyle: 'italic', color: '#e2e8f0', letterSpacing: '0.01em', background: 'rgba(255,255,255,0.02)', padding: '16px', borderRadius: '0 8px 8px 0' } }>
                  &quot;{ c.narrative_fragment }&quot;
                </div>
              </div>

              <div style={ { marginTop: 'auto', paddingTop: 'var(--space-4)' } }>
                <h4 className="text-meta" style={ { marginBottom: '10px', display: 'flex', alignItems: 'center', gap: '6px', color: 'var(--accent-secondary)' } }>
                  <AlertTriangle size={ 14 } />
                  Compensatory Rituals
                </h4>
                <p style={ { fontSize: '0.95rem', color: '#94a3b8', lineHeight: '1.6' } }>
                  { c.compensatory_rituals }
                </p>
              </div>
            </div>

            <footer style={ { marginTop: 'var(--space-6)', paddingTop: 'var(--space-4)', borderTop: '1px solid var(--border-color)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' } }>
              <span style={ { fontSize: '0.75rem', color: 'var(--text-tertiary)', fontFamily: 'monospace' } }>
                HASH: { c.source_hash.substring(0, 12) }
              </span>
              <button className="btn" style={ { padding: '6px 16px', fontSize: '0.85rem', background: 'rgba(255,255,255,0.03)', color: 'var(--text-primary)', border: '1px solid rgba(255,255,255,0.1)' } }>
                View Node Details
              </button>
            </footer>

          </article>
        )) }
      </section>

    </main>
  );
}
