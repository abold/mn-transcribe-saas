"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { supabase } from "@/lib/supabaseClient";

export default function AuthCallback() {
  const router = useRouter();

  useEffect(() => {
    // This reads the magic-link tokens from the URL,
    // stores the session in localStorage,
    // and makes the user logged in.
    supabase.auth.getSession().then(() => {
      router.replace("/");
    });
  }, [router]);

  return (
    <div className="min-h-screen flex items-center justify-center text-neutral-300">
      Logging you in…
    </div>
  );
}
