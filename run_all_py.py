import subprocess
import sys

def run_script(script_name):
    print(f"\n{'='*60}")
    print(f"🎬 LANCEMENT DU SCRIPT : {script_name}")
    print(f"{'='*60}")
    
    # Lance le script comme si tu le tapais dans le terminal
    process = subprocess.run([sys.executable, script_name], check=True)
    
    if process.returncode == 0:
        print(f"✅ {script_name} terminé avec succès.")
    else:
        print(f"❌ Erreur lors de l'exécution de {script_name}")
        sys.exit(1)

if __name__ == "__main__":
    print("🚀 DÉMARRAGE DU PIPELINE COMPLET (END-TO-END)")
    
    #etape 1 : importation des CSV sur S3 via bronze_downloader.py

    run_script("bronze_downloader.py")
    
    # ÉTAPE 2 : Transformations et S3 (Ton main.py actuel)
    run_script("main.py")
    
    # ÉTAPE 3 : Envoi vers RDS (Ton nouveau script d'automatisation)
    run_script("script_connexion_RDS.py")
    
    print("\n✨ TOUT EST À JOUR : S3 est propre et RDS est rempli !")