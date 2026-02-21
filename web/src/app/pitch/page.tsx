"use client";
import React from 'react';

export default function PitchSimulation () {
  return (
    <main style={ { padding: 'var(--space-8)', maxWidth: '1000px', margin: '0 auto', display: 'flex', flexDirection: 'column', minHeight: '100vh' } }>
      <header className="animate-fade-in" style={ { marginBottom: 'var(--space-8)' } }>
        <div style={ { display: 'inline-block', padding: '4px 12px', background: 'rgba(239, 68, 68, 0.1)', color: 'var(--error)', borderRadius: '999px', fontSize: '0.8rem', fontWeight: 600, letterSpacing: '1px', marginBottom: '12px' } }>
          SIMULATION ENGINE
        </div>
        <h1 style={ { fontSize: '2.5rem', marginBottom: 'var(--space-2)' } }>Pitch to Sufferpedia</h1>
        <p style={ { color: 'var(--text-secondary)' } }>
          Propose your product intervention. The system will embed your pitch, retrieve the most relevant Case Stories from millions of patients, and spawn <strong>Patient Agents</strong> to react to your idea.
        </p>
      </header>

      <section className="glass-panel animate-fade-in" style={ { padding: 'var(--space-6)', animationDelay: '0.1s', display: 'flex', flexDirection: 'column', gap: 'var(--space-4)' } }>
        <label style={ { fontWeight: 500, fontSize: '1.1rem', color: 'var(--text-primary)' } }>
          Describe your Intervention
        </label>
        <textarea
          placeholder="E.g., A smart-home voice-activated deadbolt system that requires zero wrist torque to unlock doors, designed for users suffering from morning stiffness..."
          rows={ 6 }
          style={ {
            width: '100%', padding: '1rem', borderRadius: 'var(--radius-md)',
            background: 'rgba(0,0,0,0.3)', border: '1px solid var(--border-color)',
            color: 'var(--text-primary)', fontSize: '1rem', outline: 'none', resize: 'vertical',
            fontFamily: 'inherit', transition: 'border-color 0.3s'
          } }
          onFocus={ (e) => e.target.style.borderColor = 'var(--accent-primary)' }
          onBlur={ (e) => e.target.style.borderColor = 'var(--border-color)' }
        />
        <div style={ { display: 'flex', justifyContent: 'flex-end', marginTop: '8px' } }>
          <button className="btn btn-primary" style={ { padding: '0.8rem 2rem', fontSize: '1.1rem' } }>
            Run Simulation →
          </button>
        </div>
      </section>

      {/* Mocking the Output Report below */ }
      <section className="animate-fade-in" style={ { marginTop: 'var(--space-12)', animationDelay: '0.2s' } }>
        <h2 style={ { fontSize: '1.5rem', marginBottom: 'var(--space-4)', borderBottom: '1px solid var(--border-color)', paddingBottom: '12px' } }>
          Simulated Agent Responses
        </h2>

        <div style={ { display: 'grid', gap: 'var(--space-4)' } }>
          {/* Reaction 1 */ }
          <div className="glass-panel" style={ { padding: 'var(--space-6)', borderLeft: '4px solid var(--error)' } }>
            <div style={ { display: 'flex', justifyContent: 'space-between', marginBottom: '12px' } }>
              <strong style={ { color: '#fff' } }>Patient Agent (Case #45091 - RA & Sjogren's)</strong>
              <span style={ { color: 'var(--error)', fontWeight: 600 } }>Rejected (2/10)</span>
            </div>
            <p style={ { color: 'var(--text-secondary)', fontStyle: 'italic', marginBottom: '12px' } }>
              "Your voice-activated deadbolt solves my morning wrist stiffness entirely, but you failed to consider my secondary conditions."
            </p>
            <div style={ { background: 'rgba(239, 68, 68, 0.05)', padding: '12px', borderRadius: 'var(--radius-sm)' } }>
              <strong>Critique based on case file:</strong> "I have severe dry throat and voice loss from Sjogren's. When I wake up, sometimes I cannot speak above a whisper for an hour. If there's an emergency, I would be locked inside my own home unable to voice-activate the door."
            </div>
          </div>

          {/* Reaction 2 */ }
          <div className="glass-panel" style={ { padding: 'var(--space-6)', borderLeft: '4px solid var(--success)' } }>
            <div style={ { display: 'flex', justifyContent: 'space-between', marginBottom: '12px' } }>
              <strong style={ { color: '#fff' } }>Patient Agent (Case #12099 - Psoriatic Arthritis)</strong>
              <span style={ { color: 'var(--success)', fontWeight: 600 } }>Accepted (9/10)</span>
            </div>
            <p style={ { color: 'var(--text-secondary)', fontStyle: 'italic', marginBottom: '12px' } }>
              "This is exactly what I need. The physical friction of turning keys is my biggest barrier to leaving the house independently."
            </p>
            <div style={ { background: 'rgba(16, 185, 129, 0.05)', padding: '12px', borderRadius: 'var(--radius-sm)' } }>
              <strong>Critique based on case file:</strong> "It perfectly addresses my lack of grip strength, and unlike Case #45091, I have no vocal comorbidities. I would buy this immediately."
            </div>
          </div>
        </div>
      </section>
    </main>
  );
}
