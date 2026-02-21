import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Sufferpedia | The Encyclopedia of Human Friction",
  description: "A visually rich, highly specific index of real medical suffering and compensatory behaviors extracted from the internet.",
};

export default function RootLayout ({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <div style={ { position: 'fixed', top: 0, left: 0, width: '100%', height: '100%', zIndex: -1, background: 'radial-gradient(circle at 50% -20%, rgba(124, 58, 237, 0.15), transparent 60%)' } } />
        { children }
      </body>
    </html>
  );
}
