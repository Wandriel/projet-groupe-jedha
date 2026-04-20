-- View: public.view_caract

-- DROP VIEW public.view_caract;

CREATE OR REPLACE VIEW public.view_caract
 AS
 SELECT r."Num_Acc",
    r.luminosite,
    r.departement,
    r.commune,
    r.agglomeration,
    r.intersection,
    r.meteo,
    r.type_collision,
    r.lat,
    r.long,
    r.date,
    r.annee,
    t.nom_total AS label_departement,
        CASE r.luminosite
            WHEN 1 THEN 'Plein jour'::text
            WHEN 2 THEN 'Crépuscule ou aube'::text
            WHEN 3 THEN 'Nuit sans éclairage public'::text
            WHEN 4 THEN 'Nuit avec éclairage public non allumé'::text
            WHEN 5 THEN 'Nuit avec éclairage public allumé'::text
            ELSE 'Inconnu'::text
        END AS luminosite_label,
        CASE r.agglomeration
            WHEN 1 THEN 'Hors agglomération'::text
            WHEN 2 THEN 'En agglomération'::text
            ELSE 'Inconnu'::text
        END AS agglomeration_label,
        CASE r.intersection
            WHEN 1 THEN 'Hors intersection'::text
            WHEN 2 THEN 'Intersection en X'::text
            WHEN 3 THEN 'Intersection en T'::text
            WHEN 4 THEN 'Intersection en Y'::text
            WHEN 5 THEN 'Intersection à plus de 4 branches'::text
            WHEN 6 THEN 'Giratoire'::text
            WHEN 7 THEN 'Place'::text
            WHEN 8 THEN 'Passage à niveau'::text
            ELSE 'Autre intersection'::text
        END AS intersection_label,
        CASE r.meteo
            WHEN 1 THEN 'Normale'::text
            WHEN 2 THEN 'Pluie légère'::text
            WHEN 3 THEN 'Pluie forte'::text
            WHEN 4 THEN 'Neige - grêle'::text
            WHEN 5 THEN 'Brouillard - fumée'::text
            WHEN 6 THEN 'Vent fort - tempête'::text
            WHEN 7 THEN 'Temps éblouissant'::text
            WHEN 8 THEN 'Temps couvert'::text
            WHEN 9 THEN 'Autre'::text
            ELSE 'Non renseigné'::text
        END AS meteo_label,
        CASE r.type_collision
            WHEN 1 THEN 'Deux véhicules - frontale'::text
            WHEN 2 THEN 'Deux véhicules – par l’arrière'::text
            WHEN 3 THEN 'Deux véhicules – par le coté'::text
            WHEN 4 THEN 'Trois véhicules et plus – en chaîne'::text
            WHEN 5 THEN 'Trois véhicules et plus - collisions multiples'::text
            WHEN 6 THEN 'Autre collision'::text
            WHEN 7 THEN 'Sans collision'::text
            ELSE 'Non renseigné'::text
        END AS type_collision_label
   FROM fact_caracteristiques r
     LEFT JOIN ref_departements t ON t.code::text = lpad(r.departement, 2, '0'::text);

ALTER TABLE public.view_caract
    OWNER TO admin_users;

