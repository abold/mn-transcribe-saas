"use client";

import { useState } from "react";
import { motion } from "framer-motion";
import { Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { createCheckout } from "@/lib/api";

type PlanKey = "free" | "creator" | "pro";

export default function PricingSection() {
  const [isYearly, setIsYearly] = useState(false);
  const [busy, setBusy] = useState<PlanKey | null>(null);
  const [err, setErr] = useState<string>("");

  const plans: Array<{
    key: PlanKey;
    name: string;
    price: string;
    yearlyPrice: string;
    description: string;
    features: string[];
    buttonText: string;
    recommended: boolean;
  }> = [
    {
      key: "free",
      name: "Free",
      price: "Free",
      yearlyPrice: "Free",
      description: "For trying the service",
      features: [
        "Max 5 minutes per file",
        "Limited monthly usage",
        "Mongolian subtitles (Cyrillic & Traditional)",
        "Download SRT",
      ],
      buttonText: "Start for free",
      recommended: false,
    },
    {
      key: "creator",
      name: "Creator",
      price: "CHF 19",
      yearlyPrice: "CHF 15",
      description: "For active content creators",
      features: [
        "Max 10 minutes per file",
        "Higher monthly transcription limit",
        "Faster processing",
        "Mongolian subtitles (Cyrillic & Traditional)",
        "Subtitle styling",
        "Download SRT",
      ],
      buttonText: "Upgrade to Creator",
      recommended: true,
    },
    {
      key: "pro",
      name: "Pro",
      price: "CHF 39",
      yearlyPrice: "CHF 31",
      description: "For professionals and teams",
      features: [
        "Max 30 minutes per file",
        "Highest monthly transcription limit",
        "Priority processing",
        "Mongolian + multilingual subtitles",
        "Subtitle styling",
        "Download SRT",
      ],
      buttonText: "Upgrade to Pro",
      recommended: false,
    },
  ];

  async function onCTA(plan: PlanKey) {
    setErr("");

    if (plan === "free") {
      // Scroll to uploader section if present
      document.getElementById("uploader")?.scrollIntoView({ behavior: "smooth" });
      return;
    }

    try {
      setBusy(plan);

      // NOTE: You only have monthly Stripe prices right now.
      // So we always checkout monthly even if toggle is "Yearly".
      const { url } = await createCheckout(plan);
      window.location.href = url;
    } catch (e: any) {
      setErr(e?.message || "Checkout failed");
    } finally {
      setBusy(null);
    }
  }

  return (
    <section className="py-24 px-4 relative overflow-hidden">
      {/* Background glow effects */}
      <div className="absolute inset-0 opacity-50" />
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[800px] h-[600px] bg-[radial-gradient(circle,rgba(99,102,241,0.12),transparent_60%)] blur-3xl" />

      <div className="max-w-6xl mx-auto relative z-10">
        {/* Section Header */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6 }}
          className="text-center mb-12"
        >
          <h2 className="text-4xl md:text-5xl font-bold mb-4">
            <span className="bg-clip-text text-transparent bg-gradient-to-r from-white to-white/60">
              Pricing
            </span>
          </h2>
          <p className="text-muted-foreground text-lg max-w-2xl mx-auto">
            Choose the plan that fits your creative workflow
          </p>
          {err ? <p className="mt-3 text-sm text-red-400">{err}</p> : null}
        </motion.div>

        {/* Billing Toggle (UI only for now) */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6, delay: 0.1 }}
          className="flex items-center justify-center gap-4 mb-12"
        >
          <span
            className={`text-sm font-medium transition-colors ${
              !isYearly ? "text-foreground" : "text-muted-foreground"
            }`}
          >
            Monthly
          </span>

          <button
            onClick={() => setIsYearly(!isYearly)}
            className={`relative w-14 h-7 rounded-full transition-colors duration-300 ${
              isYearly ? "bg-primary" : "bg-secondary"
            }`}
            aria-label="Toggle billing cycle"
          >
            <span
              className={`absolute top-1 left-1 w-5 h-5 rounded-full bg-foreground transition-transform duration-300 ${
                isYearly ? "translate-x-7" : "translate-x-0"
              }`}
            />
          </button>

          <span
            className={`text-sm font-medium transition-colors ${
              isYearly ? "text-foreground" : "text-muted-foreground"
            }`}
          >
            Yearly
          </span>

          {isYearly && (
            <Badge
              variant="default"
              className="bg-primary/20 text-primary border-primary/30 text-xs"
            >
              Save 20%
            </Badge>
          )}
        </motion.div>

        {/* Pricing Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 lg:gap-8">
          {plans.map((plan, index) => (
            <motion.div
              key={plan.key}
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ duration: 0.5, delay: index * 0.1 }}
              className={`relative group ${plan.recommended ? "md:-mt-4 md:mb-4" : ""}`}
            >
              {/* Recommended glow effect */}
              {plan.recommended && (
                <div className="absolute -inset-[1px] bg-gradient-to-r from-primary via-purple-500 to-primary rounded-2xl opacity-70 blur-sm group-hover:opacity-100 transition-opacity duration-500" />
              )}

              <div
                className={`relative h-full rounded-2xl border bg-card/60 backdrop-blur p-6 lg:p-8 transition-all duration-500 ${
                  plan.recommended
                    ? "border-primary/30 shadow-lg"
                    : "border-border hover:border-white/[0.12] hover:bg-card/80"
                }`}
              >
                {/* Recommended Badge */}
                {plan.recommended && (
                  <div className="absolute -top-3 left-1/2 -translate-x-1/2">
                    <Badge className="bg-gradient-to-r from-primary to-purple-500 text-primary-foreground border-0 px-4 py-1">
                      Recommended
                    </Badge>
                  </div>
                )}

                {/* Plan Header */}
                <div className="mb-6">
                  <h3 className="text-xl font-semibold text-foreground mb-2">
                    {plan.name}
                  </h3>
                  <p className="text-muted-foreground text-sm">{plan.description}</p>
                </div>

                {/* Price */}
                <div className="mb-6">
                  <div className="flex items-baseline gap-1">
                    <span className="text-4xl font-bold text-foreground">
                      {isYearly ? plan.yearlyPrice : plan.price}
                    </span>
                    {plan.price !== "Free" && (
                      <span className="text-muted-foreground text-sm">/ month</span>
                    )}
                  </div>

                  {/* Note: your Stripe is monthly-only for now */}
                  {isYearly && plan.price !== "Free" && (
                    <p className="text-xs text-muted-foreground mt-1">
                      (UI preview only — checkout is monthly for now)
                    </p>
                  )}
                </div>

                {/* Features */}
                <ul className="space-y-3 mb-8">
                  {plan.features.map((feature, featureIndex) => (
                    <li key={featureIndex} className="flex items-start gap-3">
                      <div
                        className={`mt-0.5 rounded-full p-1 ${
                          plan.recommended
                            ? "bg-primary/20 text-primary"
                            : "bg-secondary text-muted-foreground"
                        }`}
                      >
                        <Check className="w-3 h-3" />
                      </div>
                      <span className="text-sm text-muted-foreground">{feature}</span>
                    </li>
                  ))}
                </ul>

                {/* CTA Button */}
                <Button
                  variant={plan.key === "free" ? "outline" : "default"}
                  size="lg"
                  className={`w-full ${
                    plan.key === "free"
                      ? "border-border hover:border-primary/50 hover:bg-primary/5"
                      : plan.recommended
                      ? "bg-primary hover:bg-primary/90"
                      : ""
                  }`}
                  onClick={() => onCTA(plan.key)}
                  disabled={busy === plan.key}
                >
                  {busy === plan.key ? "Redirecting…" : plan.buttonText}
                </Button>
              </div>
            </motion.div>
          ))}
        </div>

        {/* Footer Note */}
        <motion.p
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6, delay: 0.5 }}
          className="text-center text-muted-foreground text-sm mt-12"
        >
          All plans include secure cloud storage and 24/7 availability
        </motion.p>
      </div>
    </section>
  );
}
